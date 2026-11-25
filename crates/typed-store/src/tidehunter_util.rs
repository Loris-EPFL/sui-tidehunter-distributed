// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{DBMetrics, StorageType, rocks::MetricConf, util::ensure_database_type};
use bincode::Options;
use mysten_common::ZipDebugEqIteratorExt;
use mysten_metrics::RegistryID;
use prometheus::{HistogramTimer, Registry};
use serde::de::DeserializeOwned;
use std::env;
use std::path::Path;
use std::sync::Arc;
use tidehunter::compressed_batch::BatchCodec;
use tidehunter::config::Config;
pub use tidehunter::distributed::sui::Db;
use tidehunter::distributed::sui::DbIterator;
use tidehunter::key_shape::KeyShape;
use tidehunter::metrics::Metrics;
pub use tidehunter::{
    Decision, WalPosition,
    key_shape::{KeyIndexing, KeyShapeBuilder, KeySpaceConfig, KeyType},
    minibytes::Bytes,
};
use typed_store_error::TypedStoreError;

#[derive(Clone)]
pub struct ThConfig {
    key_indexing: KeyIndexing,
    key_type: KeyType,
    mutexes: usize,
    config: KeySpaceConfig,
    pub prefix: Option<Vec<u8>>,
}

pub fn open(
    path: &Path,
    key_shape: KeyShape,
    metric_conf: &MetricConf,
) -> (Arc<Db>, RegistryID, AdapterMetricsSampler) {
    std::fs::create_dir_all(path).expect("failed to open tidehunter db");
    let registry_service = &DBMetrics::get().registry_serivce;
    let registry = new_db_registry(metric_conf.db_name.clone());
    let registry_id = registry_service.add(registry.clone());
    let metrics = Metrics::new_in(&registry);
    ensure_database_type(path, StorageType::TideHunter).expect("failed to open tidehunter db");
    // `Db::open` reconciles the declared shape against the persisted registry
    // (matching by name; supports reorder/add/remove). Callers resolve each
    // column family's handle by name via `Db::ks`.
    let db = open_backend(path, key_shape, metric_conf, metrics);
    if let Some(native) = db.native() {
        destroy_undeclared_key_spaces(native, path);
        native.start_periodic_snapshot();
    }
    db.set_native_durable(parse_env_bool("TH_SYNC_WRITES").unwrap_or(false));
    let read_profile_limit = env::var("SUI_TH_READ_PROFILE_LIMIT")
        .map(|value| {
            value
                .parse::<u64>()
                .expect("SUI_TH_READ_PROFILE_LIMIT must be a u64")
        })
        .unwrap_or(0);
    db.set_read_profile_limit(read_profile_limit);
    let native_write_profile_limit = env::var("SUI_TH_NATIVE_WRITE_PROFILE_LIMIT")
        .map(|value| {
            value
                .parse::<u64>()
                .expect("SUI_TH_NATIVE_WRITE_PROFILE_LIMIT must be a u64")
        })
        .unwrap_or(0);
    db.set_native_write_profile_limit(native_write_profile_limit);
    export_schema(path, &db);
    let sampler = register_remote_metrics(&registry, &db, path);
    (db, registry_id, sampler)
}

#[cfg(feature = "sharded-wal")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ShardedWalConfig {
    root: std::path::PathBuf,
    databases: std::collections::BTreeMap<String, ShardedDatabaseConfig>,
}

/// One namespace of `SUI_SHARDED_WAL_CONFIG`, with the presence of its hybrid
/// index section recorded: a document that states one keeps the operator's
/// values, and an absent section takes Sui's resolved native index rows.
#[cfg(feature = "sharded-wal")]
struct ShardedDatabaseConfig {
    remote: tidehunter::distributed::remote_db::RemoteDatabaseConfig,
    index_section: bool,
}

#[cfg(feature = "sharded-wal")]
impl<'de> serde::Deserialize<'de> for ShardedDatabaseConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let document = <serde_json::Value as serde::Deserialize>::deserialize(deserializer)?;
        let index_section = document.get("index").is_some();
        // `RemoteDatabaseConfig` keeps `deny_unknown_fields`, so a misspelled
        // namespace row still fails at parse.
        let remote = serde_json::from_value(document).map_err(serde::de::Error::custom)?;
        Ok(Self {
            remote,
            index_section,
        })
    }
}

#[cfg(feature = "sharded-wal")]
fn sharded_config() -> Option<ShardedWalConfig> {
    env::var_os("SUI_SHARDED_WAL_CONFIG").map(|path| {
        let bytes = std::fs::read(path).expect("read SUI_SHARDED_WAL_CONFIG");
        serde_json::from_slice(&bytes).expect("parse SUI_SHARDED_WAL_CONFIG")
    })
}

/// The remote route takes its write-acknowledgement tier from
/// `SUI_SHARDED_WAL_CONFIG`, so a `TH_SYNC_WRITES` value naming another tier
/// would measure an arm the operator did not select. Absent keeps the
/// document's tier; a disagreement names both tiers and fails the open.
#[cfg(feature = "sharded-wal")]
fn remote_durability_mismatch(
    configured: tidehunter::distributed::durability::DurabilityTier,
    sync_writes: Option<bool>,
) -> Option<String> {
    let sync_writes = sync_writes?;
    let requested = if sync_writes {
        tidehunter::distributed::durability::DurabilityTier::Synchronous
    } else {
        tidehunter::distributed::durability::DurabilityTier::Native
    };
    if configured == requested {
        return None;
    }
    Some(format!(
        "TH_SYNC_WRITES={sync_writes} requires live_nvme.leased_durability \"{}\" in \
         SUI_SHARDED_WAL_CONFIG, but the document sets \"{}\"",
        requested.as_str(),
        configured.as_str()
    ))
}

fn configured_root() -> Option<std::path::PathBuf> {
    #[cfg(feature = "sharded-wal")]
    if let Some(config) = sharded_config() {
        return Some(config.root);
    }
    env::var_os("SUI_SHARDED_WAL_ROOT").map(Into::into)
}

fn namespace(path: &Path, root: &Path) -> String {
    let root = root.canonicalize().expect("canonicalize Sui database root");
    let path = path.canonicalize().expect("canonicalize Sui database path");
    let relative = path
        .strip_prefix(root)
        .expect("Tidehunter DB outside configured Sui root");
    assert!(
        !relative.as_os_str().is_empty(),
        "DB namespace must not be empty"
    );
    relative.to_str().expect("UTF-8 DB namespace").to_owned()
}

fn open_backend(
    path: &Path,
    shape: KeyShape,
    metric_conf: &MetricConf,
    metrics: Arc<Metrics>,
) -> Arc<Db> {
    #[cfg(feature = "sharded-wal")]
    {
        let config = sharded_config()
            .expect("sharded-wal requires SUI_SHARDED_WAL_CONFIG; native fallback is forbidden");
        let name = namespace(path, &config.root);
        let database = config
            .databases
            .get(&name)
            .unwrap_or_else(|| panic!("missing exact sharded-wal namespace {name:?}"));
        let remote = if database.index_section {
            database.remote.clone()
        } else {
            // D47: the coordinator's hybrid index runs Sui's resolved native
            // index rows, the same values the native path gives `Db::open`.
            let native = thdb_config(metric_conf);
            database.remote.clone().with_native_index(&native)
        };
        if let Some(reason) = remote_durability_mismatch(
            remote.live_nvme.leased_durability,
            parse_env_bool("TH_SYNC_WRITES"),
        ) {
            panic!("{reason}");
        }
        Db::open_remote_with_metrics(path, shape, remote, metrics)
            .expect("open sharded-wal Sui database")
    }
    #[cfg(not(feature = "sharded-wal"))]
    {
        assert!(
            env::var_os("SUI_SHARDED_WAL_CONFIG").is_none(),
            "SUI_SHARDED_WAL_CONFIG requires the sharded-wal feature"
        );
        assert!(
            !path.join("tcp-database-authority-v1").exists(),
            "native control cannot open a sharded-wal authority"
        );
        Db::open_native(path, shape, Arc::new(thdb_config(metric_conf)), metrics)
            .expect("open native Tidehunter database")
    }
}

fn export_schema(path: &Path, db: &Db) {
    let Some(output) = env::var_os("SUI_SHARDED_WAL_SCHEMA_OUTPUT") else {
        return;
    };
    let root = configured_root()
        .expect("schema export requires SUI_SHARDED_WAL_ROOT or remote config root");
    let name = namespace(path, &root);
    let output = std::path::PathBuf::from(output).join(&name);
    std::fs::create_dir_all(&output).expect("create schema output directory");
    std::fs::write(
        output.join("shape.json"),
        serde_json::to_vec_pretty(db.shape()).expect("serialize key shape"),
    )
    .expect("export key shape");
    std::fs::write(
        output.join("shape.yaml"),
        db.shape_yaml().expect("serialize canonical key shape YAML"),
    )
    .expect("export canonical key shape YAML");
    let manifest = serde_json::json!({
        "namespace": name, "path": path,
        "backend": if db.native().is_some() { "native" } else { "sharded-wal" }
    });
    std::fs::write(
        output.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("serialize schema manifest"),
    )
    .expect("export schema manifest");
}

#[derive(Clone)]
struct KeyspaceCacheGauges {
    hits: prometheus::IntGauge,
    misses: prometheus::IntGauge,
    inserts: prometheus::IntGauge,
    evictions: prometheus::IntGauge,
}

#[derive(Clone)]
struct AdapterCollector {
    values: prometheus::IntGaugeVec,
    keyspace_cache: Option<(prometheus::IntGaugeVec, Box<[KeyspaceCacheGauges]>)>,
    local_available: std::sync::Arc<std::sync::atomic::AtomicBool>,
    local_generation: std::sync::Arc<std::sync::atomic::AtomicU64>,
    local_last_success: std::sync::Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    local_stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    metrics_lock: std::sync::Arc<std::sync::Mutex<()>>,
}
impl prometheus::core::Collector for AdapterCollector {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        let mut desc = self.values.desc();
        if let Some((keyspace_cache, _)) = &self.keyspace_cache {
            desc.extend(keyspace_cache.desc());
        }
        desc
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        let _metrics = self
            .metrics_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.update_local_sample_status();
        let mut families = self.values.collect();
        if self
            .local_available
            .load(std::sync::atomic::Ordering::Acquire)
            && let Some((keyspace_cache, _)) = &self.keyspace_cache
        {
            families.extend(keyspace_cache.collect());
        }
        families
    }
}
impl AdapterCollector {
    fn update_local_sample_status(&self) {
        let stopped = self
            .local_stopped
            .load(std::sync::atomic::Ordering::Acquire);
        let last_success = self
            .local_last_success
            .try_lock()
            .ok()
            .and_then(|guard| *guard);
        let fresh = last_success.is_some_and(|attempt| {
            let age = attempt.elapsed();
            self.values
                .with_label_values(&["local_sample_age_ms"])
                .set(age.as_millis().min(i64::MAX as u128) as i64);
            age <= std::time::Duration::from_secs(3)
        });
        if last_success.is_none() {
            let _ = self.values.remove_label_values(&["local_sample_age_ms"]);
        }
        let available = fresh
            && self
                .local_available
                .load(std::sync::atomic::Ordering::Acquire)
            && !stopped;
        self.values
            .with_label_values(&["local_sample_available"])
            .set(if available { 1 } else { 0 });
        self.values
            .with_label_values(&["local_sampler_stopped"])
            .set(if stopped { 1 } else { 0 });
        self.local_available
            .store(available, std::sync::atomic::Ordering::Release);
    }

    fn update(&self, db: &Db) -> bool {
        let _metrics = self
            .metrics_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.update_locked(db)
    }

    fn update_locked(&self, db: &Db) -> bool {
        use std::sync::atomic::Ordering;
        self.local_available.store(false, Ordering::Release);
        self.values.reset();
        let mut set = |name: &str, value: u64| {
            self.values
                .with_label_values(&[name])
                .set(value.min(i64::MAX as u64) as i64);
        };
        let generation = self
            .local_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                Some(generation.saturating_add(1))
            })
            .unwrap_or_else(|generation| generation)
            .saturating_add(1);
        set("local_sample_generation", generation);
        set("local_sample_available", 0);
        set("local_sampler_stopped", 0);
        set("read_calls", db.read_calls.load(Ordering::Relaxed));
        set("read_keys", db.read_keys.load(Ordering::Relaxed));
        set("scan_records", db.scan_records.load(Ordering::Relaxed));
        if db.try_visit_timing_metrics(&mut set).is_none() {
            self.invalidate_locked();
            return false;
        }
        let Some(remote_counters) = db.try_remote_counters() else {
            self.invalidate_locked();
            return false;
        };
        if let Some((cache, writes, rpcs)) = remote_counters {
            set("read_rpcs", rpcs);
            set("cache_hits", cache.hits);
            set("cache_misses", cache.misses);
            set("cache_evictions", cache.evictions);
            set("cache_retained_bytes", cache.retained_bytes as u64);
            set("cache_budget_bytes", cache.budget_bytes as u64);
            set("cache_metadata_bytes", cache.metadata_bytes as u64);
            set(
                "cache_per_keyspace_counters_bytes",
                cache.per_keyspace_counters_bytes as u64,
            );
            set("cache_served_bytes", cache.served_bytes);
            set("published_batches", writes.published_batches);
            set("write_validation_ns", writes.validation_ns);
            set("write_storage_wait_ns", writes.storage_wait_ns);
            set("write_publication_ns", writes.publication_ns);
            set("read_index_ns", writes.read_resolve_ns);
            set("read_wait_ns", writes.value_read_ns);
        }
        let Some(keyspace_counters) = db.try_remote_keyspace_cache_counters() else {
            self.invalidate_locked();
            return false;
        };
        if let (Some(counters), Some((_, gauges))) = (keyspace_counters, &self.keyspace_cache) {
            // The fixed counter array also contains undeclared keyspace IDs.
            for (gauge, stats) in gauges.iter().zip_debug_eq(&counters[..gauges.len()]) {
                gauge.hits.set(stats.hits.min(i64::MAX as u64) as i64);
                gauge.misses.set(stats.misses.min(i64::MAX as u64) as i64);
                gauge.inserts.set(stats.inserts.min(i64::MAX as u64) as i64);
                gauge
                    .evictions
                    .set(stats.evictions.min(i64::MAX as u64) as i64);
            }
        }
        let Ok(mut last_success) = self.local_last_success.lock() else {
            self.invalidate_locked();
            return false;
        };
        *last_success = Some(std::time::Instant::now());
        drop(last_success);
        let previous = self.local_generation.load(Ordering::Acquire);
        set("local_sample_generation", previous);
        set("local_sample_available", 1);
        self.local_available.store(true, Ordering::Release);
        true
    }
    fn invalidate_locked(&self) {
        self.local_available
            .store(false, std::sync::atomic::Ordering::Release);
        self.values.reset();
        let generation = self
            .local_generation
            .load(std::sync::atomic::Ordering::Acquire);
        if generation != 0 {
            self.values
                .with_label_values(&["local_sample_generation"])
                .set(generation.min(i64::MAX as u64) as i64);
        }
        self.values
            .with_label_values(&["local_sample_available"])
            .set(0);
        self.values
            .with_label_values(&["local_sampler_stopped"])
            .set(0);
    }

    fn mark_stopped(&self) {
        self.local_stopped
            .store(true, std::sync::atomic::Ordering::Release);
        let _metrics = self
            .metrics_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.invalidate_locked();
        self.values
            .with_label_values(&["local_sampler_stopped"])
            .set(1);
    }
}
pub struct AdapterMetricsSampler {
    stop: std::sync::mpsc::SyncSender<()>,
    worker: Option<std::thread::JoinHandle<()>>,
    local_stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    metrics_lock: std::sync::Arc<std::sync::Mutex<()>>,
    values: prometheus::IntGaugeVec,
}
impl std::fmt::Debug for AdapterMetricsSampler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdapterMetricsSampler")
            .field("running", &self.worker.is_some())
            .finish()
    }
}

impl AdapterMetricsSampler {
    pub fn stop(&mut self) {
        self.local_stopped
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = self.stop.try_send(());
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            tracing::error!("Tidehunter adapter metrics sampler panicked during shutdown");
        }
        let _metrics = self
            .metrics_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.values
            .with_label_values(&["local_sample_available"])
            .set(0);
        self.values
            .with_label_values(&["local_sampler_stopped"])
            .set(1);
    }
}
impl Drop for AdapterMetricsSampler {
    fn drop(&mut self) {
        self.stop();
    }
}

fn register_remote_metrics(
    registry: &Registry,
    db: &Arc<Db>,
    path: &Path,
) -> AdapterMetricsSampler {
    let root = configured_root();
    let name = root.map_or_else(|| path.display().to_string(), |root| namespace(path, &root));
    let values = prometheus::IntGaugeVec::new(
        prometheus::Opts::new(
            "tidehunter_adapter",
            "Cumulative adapter counters and current bounded-cache gauges",
        )
        .const_label("namespace", name.clone()),
        &["stat"],
    )
    .expect("adapter metric definition");
    values.with_label_values(&["local_sample_available"]).set(0);
    values.with_label_values(&["local_sampler_stopped"]).set(0);
    let sampler_values = values.clone();
    // The coordinator's per-keyspace read cache exists only on the remote
    // backend; the native keyspaces report no such counters.
    let keyspace_cache = db.native().is_none().then(|| {
        let cache = prometheus::IntGaugeVec::new(
            prometheus::Opts::new(
                "tidehunter_keyspace_read_cache",
                "Cumulative read-cache counters by canonical Tidehunter keyspace id",
            )
            .const_label("namespace", name.clone()),
            &["keyspace", "keyspace_id", "stat"],
        )
        .expect("keyspace read-cache metric definition");
        let gauges = db
            .shape()
            .iter_ks()
            .enumerate()
            .take(tidehunter::distributed::remote_db::KEYSPACE_COUNTER_COUNT)
            .map(|(id, descriptor)| {
                let id = id.to_string();
                let gauge = |stat| cache.with_label_values(&[descriptor.name(), &id, stat]);
                KeyspaceCacheGauges {
                    hits: gauge("hits"),
                    misses: gauge("misses"),
                    inserts: gauge("inserts"),
                    evictions: gauge("evictions"),
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        (cache, gauges)
    });
    let local_available = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let local_generation = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let local_last_success = std::sync::Arc::new(std::sync::Mutex::new(None));
    let local_stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let metrics_lock = std::sync::Arc::new(std::sync::Mutex::new(()));
    let collector = AdapterCollector {
        values,
        keyspace_cache,
        local_available,
        local_generation,
        local_last_success,
        local_stopped: local_stopped.clone(),
        metrics_lock: metrics_lock.clone(),
    };
    registry
        .register(Box::new(collector.clone()))
        .expect("register Tidehunter adapter counters");
    // The engine registers its own live NVMe-oF counters into the caller's
    // registry. A failure here costs observability, not storage, so it is
    // reported and does not fail the open.
    if db.native().is_none()
        && let Err(error) = db.register_nvmeof_metrics(registry)
    {
        tracing::error!(?error, "failed to register Tidehunter NVMe-oF live metrics");
    }

    let weak_db = Arc::downgrade(db);
    let (stop, stop_receiver) = std::sync::mpsc::sync_channel(1);
    let worker_collector = collector.clone();
    let spawn_result = std::thread::Builder::new()
        .name("tidehunter-metrics".to_owned())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                loop {
                    if stop_receiver
                        .recv_timeout(std::time::Duration::from_secs(1))
                        .is_ok()
                    {
                        worker_collector.mark_stopped();
                        break;
                    }
                    match weak_db.upgrade() {
                        Some(db) => {
                            worker_collector.update(&db);
                        }
                        None => {
                            worker_collector.mark_stopped();
                            break;
                        }
                    }
                }
            }));
            if result.is_err() {
                worker_collector.mark_stopped();
                tracing::error!("Tidehunter adapter metrics sampler panicked");
            }
        });
    let worker = match spawn_result {
        Ok(worker) => Some(worker),
        Err(error) => {
            collector.mark_stopped();
            tracing::error!(%error, "failed to start Tidehunter adapter metrics sampler");
            None
        }
    };
    AdapterMetricsSampler {
        stop,
        worker,
        values: sampler_values,
        local_stopped,
        metrics_lock,
    }
}

/// Destroys key spaces that exist in the database but are no longer declared
/// in the `KeyShape`, i.e. tables that were removed from the DBMap struct.
/// `Db::open` retains such key spaces internally (data preserved, not
/// exposed); destroying them persists a registry tombstone so their data is
/// dropped and disk space is reclaimed through the regular GC cadences.
fn destroy_undeclared_key_spaces(db: &tidehunter::db::Db, path: &Path) {
    let registry = tidehunter::db::Db::load_key_shape(path)
        .expect("failed to load tidehunter key shape registry");
    for ks in registry.iter_ks() {
        if ks.destroyed() || db.try_ks(ks.name()).is_some() {
            continue;
        }
        tracing::warn!(
            "destroying tidehunter key space '{}': present in database but not declared",
            ks.name()
        );
        db.destroy_key_space(ks.name())
            .expect("failed to destroy tidehunter key space");
    }
}

fn new_db_registry(name: String) -> Registry {
    let labels = [("db".to_string(), name)].into_iter().collect();
    Registry::new_custom(None, Some(labels)).expect("failed to create registry")
}

pub fn add_key_space(builder: &mut KeyShapeBuilder, name: &str, config: &ThConfig) {
    // `add_key_space_config_indexing` returns `&mut KeyShapeBuilder`; the
    // canonical handle is no longer assigned here. It is resolved by name from
    // the `KeySpaces` returned by `open` (see the DBMapUtils derive macro).
    builder.add_key_space_config_indexing(
        name,
        config.key_indexing.clone(),
        config.mutexes,
        config.key_type,
        config.config.clone(),
    );
}

fn parse_env_usize(name: &str, minimum: usize) -> Option<usize> {
    env::var(name).ok().map(|value| {
        let parsed = value
            .parse::<usize>()
            .unwrap_or_else(|_| panic!("Failed to parse {name}"));
        assert!(parsed >= minimum, "{name} must be at least {minimum}");
        println!("Using {name} from env variable {parsed}");
        parsed
    })
}

fn parse_env_bool(name: &str) -> Option<bool> {
    env::var(name).ok().map(|value| {
        let parsed = value
            .parse::<bool>()
            .unwrap_or_else(|_| panic!("Failed to parse {name}"));
        println!("Using {name} from env variable {parsed}");
        parsed
    })
}

pub fn default_max_dirty_keys() -> usize {
    parse_env_usize("TH_DEFAULT_MAX_DIRTY_KEYS", 1).unwrap_or(1024)
}

/// Sui's resolved native index rows. The native backend hands them to
/// `Db::open`; the sharded-wal backend hands them to the coordinator hybrid
/// through [`tidehunter::distributed::remote_db::RemoteDatabaseConfig::with_native_index`].
fn thdb_config(metric_conf: &MetricConf) -> Config {
    let frag_size = if let Ok(frag_size) = env::var("TH_FRAG_SIZE") {
        let frag_size = frag_size.parse().expect("Failed to parse TH_FRAG_SIZE");
        assert!(frag_size > 0, "TH_FRAG_SIZE must be more then 0");
        assert!(
            frag_size % (4 * 1024) == 0,
            "TH_FRAG_SIZE must be page aligned({frag_size} given)"
        );
        println!("Using frag size from env variable {frag_size}");
        frag_size
    } else {
        #[cfg(debug_assertions)]
        {
            32 * 1024 * 1024
        } // 32 Mb for tests
        #[cfg(not(debug_assertions))]
        {
            1024 * 1024 * 1024
        } // 1 Gb for prod
    };
    #[cfg(debug_assertions)]
    let max_maps = 4;
    #[cfg(not(debug_assertions))]
    let max_maps = 8; // 8Gb of mapped space for prod
    let max_maps = parse_env_usize("TH_MAX_MAPS", 1).unwrap_or(max_maps);
    let max_maps = if metric_conf.db_name.starts_with("perpetual") {
        max_maps * 2
    } else {
        max_maps
    };
    let max_index_maps = Some(parse_env_usize("TH_MAX_INDEX_MAPS", 3).unwrap_or(3));
    #[cfg(debug_assertions)]
    let commit_pool_size = 0;
    #[cfg(not(debug_assertions))]
    let commit_pool_size = 8; // Use thread pool to commit large batches
    #[cfg(debug_assertions)]
    let num_flusher_threads = 1;
    #[cfg(not(debug_assertions))]
    let num_flusher_threads = 4;
    let batch_codec = metric_conf
        .enable_th_batch_compression
        .then_some(BatchCodec::Lz4);
    let compressed_batch_cache_bytes = metric_conf
        .enable_th_batch_compression
        .then_some(4 * 1024 * 1024 * 1024);
    let mut config = Config {
        frag_size,
        // run snapshot every 8 Gb written to wal
        snapshot_written_bytes: parse_env_usize("TH_SNAPSHOT_WRITTEN_BYTES", 1)
            .unwrap_or(8 * 1024 * 1024 * 1024) as u64,
        // force unloading dirty index entries if behind 7.5 Gb of wal
        snapshot_unload_threshold: parse_env_usize("TH_SNAPSHOT_UNLOAD_THRESHOLD", 1)
            .unwrap_or(15 * 1024 * 1024 * 1024 / 2) as u64,
        unload_jitter_pct: 30,
        max_dirty_keys: default_max_dirty_keys(),
        max_maps,
        max_index_maps,
        commit_pool_size,
        num_flusher_threads,
        batch_codec,
        compressed_batch_cache_bytes,
        relocation_max_reclaim_pct: 100,
        ..Config::default()
    };

    apply_auto_sharding(&mut config, parse_env_bool("TH_AUTOSHARD"));

    config
}

/// `TH_AUTOSHARD` selects the index auto-sharding mode. Absent keeps the
/// default `frag_size / 2` threshold, `true` restores it explicitly, and
/// `false` disables auto-sharding for the campaign arms that compare the two
/// layouts. A sharded-wal document without an `index` section takes these
/// native rows through `RemoteDatabaseConfig::with_native_index`; a document
/// that states the section keeps the operator's value.
fn apply_auto_sharding(config: &mut Config, autoshard: Option<bool>) {
    match autoshard {
        Some(true) => config.with_index_auto_sharding(),
        Some(false) => config.disable_index_auto_sharding(),
        None => {}
    }
}

#[cfg(not(debug_assertions))]
pub fn default_mutex_count() -> usize {
    parse_env_usize("TH_DEFAULT_MUTEX_COUNT", 1).unwrap_or(1024)
}

#[cfg(debug_assertions)]
pub fn default_mutex_count() -> usize {
    parse_env_usize("TH_DEFAULT_MUTEX_COUNT", 1).unwrap_or(16)
}

/// Sui's per-keyspace value cache entries: a campaign-wide `TH_VALUE_CACHE_SIZE`
/// override first, then the `TH_DEFAULT_VALUE_CACHE_SIZE` default of 1000. Zero
/// disables the cache. Both the native per-cell cache and the sharded-wal
/// coordinator's per-cell cache read this size from the keyspace shape.
pub fn default_value_cache_size() -> usize {
    value_cache_size_override(parse_env_usize("TH_DEFAULT_VALUE_CACHE_SIZE", 1).unwrap_or(1000))
}

/// The campaign override for a keyspace whose cache size Sui sets explicitly
/// instead of taking [`default_value_cache_size`]: `TH_VALUE_CACHE_SIZE`
/// replaces the requested size, and zero disables the cache.
pub fn value_cache_size_override(requested: usize) -> usize {
    parse_env_usize("TH_VALUE_CACHE_SIZE", 0).unwrap_or(requested)
}

pub(crate) fn apply_range_bounds(
    iterator: &mut DbIterator,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
    prefix: &Option<Vec<u8>>,
) {
    // Bounds come from `be_fix_int_ser(K)` which still includes the typed-store
    // length prefix. Tidehunter stores keys with the prefix stripped, so the
    // bounds must be stripped to match — same transform every other key path uses.
    if let Some(lower_bound) = lower_bound {
        iterator.set_lower_bound(transform_th_key(&lower_bound, prefix).to_vec());
    }
    if let Some(upper_bound) = upper_bound {
        iterator.set_upper_bound(transform_th_key(&upper_bound, prefix).to_vec());
    }
}

pub(crate) fn transform_th_iterator<'a, K, V>(
    iterator: impl Iterator<
        Item = Result<
            (tidehunter::minibytes::Bytes, tidehunter::minibytes::Bytes),
            tidehunter::db::DbError,
        >,
    > + 'a,
    prefix: &'a Option<Vec<u8>>,
    timer: HistogramTimer,
) -> impl Iterator<Item = Result<(K, V), TypedStoreError>> + 'a
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    let config = bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding();
    iterator.map(move |item| {
        item.map_err(|e| TypedStoreError::RocksDBError(format!("tidehunter error {:?}", e)))
            .and_then(|(raw_key, raw_value)| {
                let _timer = &timer;
                let key = match prefix {
                    Some(prefix) => {
                        let mut buffer = Vec::with_capacity(raw_key.len() + prefix.len());
                        buffer.extend_from_slice(prefix);
                        buffer.extend_from_slice(&raw_key);
                        config.deserialize(&buffer)
                    }
                    None => config.deserialize(&raw_key),
                };
                let value = bcs::from_bytes(&raw_value);
                match (key, value) {
                    (Ok(k), Ok(v)) => Ok((k, v)),
                    (Err(e), _) => Err(TypedStoreError::SerializationError(e.to_string())),
                    (_, Err(e)) => Err(TypedStoreError::SerializationError(e.to_string())),
                }
            })
    })
}

/// Borrow the stored-key suffix; scalar reads do not need a second key allocation.
pub(crate) fn transform_th_key<'a>(key: &'a [u8], prefix: &Option<Vec<u8>>) -> &'a [u8] {
    match prefix {
        Some(prefix) => &key[prefix.len()..],
        None => key,
    }
}

pub(crate) fn typed_store_error_from_th_error(err: tidehunter::db::DbError) -> TypedStoreError {
    TypedStoreError::RocksDBError(format!("tidehunter error: {:?}", err))
}

impl ThConfig {
    pub fn new(key_size: usize, mutexes: usize, key_type: KeyType) -> Self {
        Self {
            key_indexing: KeyIndexing::fixed(key_size),
            mutexes,
            key_type,
            config: KeySpaceConfig::default(),
            prefix: None,
        }
    }
    pub fn new_with_indexing(key_indexing: KeyIndexing, mutexes: usize, key_type: KeyType) -> Self {
        Self {
            key_indexing,
            mutexes,
            key_type,
            config: KeySpaceConfig::default(),
            prefix: None,
        }
    }

    pub fn new_with_config_indexing(
        key_indexing: KeyIndexing,
        mutexes: usize,
        key_type: KeyType,
        config: KeySpaceConfig,
    ) -> Self {
        Self {
            key_indexing,
            mutexes,
            key_type,
            config,
            prefix: None,
        }
    }

    pub fn new_with_config(
        key_size: usize,
        mutexes: usize,
        key_type: KeyType,
        config: KeySpaceConfig,
    ) -> Self {
        Self::new_with_config_indexing(KeyIndexing::fixed(key_size), mutexes, key_type, config)
    }

    pub fn new_with_rm_prefix(
        key_size: usize,
        mutexes: usize,
        key_type: KeyType,
        config: KeySpaceConfig,
        prefix: Vec<u8>,
    ) -> Self {
        Self::new_with_rm_prefix_indexing(
            KeyIndexing::fixed(key_size),
            mutexes,
            key_type,
            config,
            prefix,
        )
    }

    pub fn new_with_rm_prefix_indexing(
        key_indexing: KeyIndexing,
        mutexes: usize,
        key_type: KeyType,
        config: KeySpaceConfig,
        prefix: Vec<u8>,
    ) -> Self {
        Self {
            key_indexing,
            mutexes,
            key_type,
            config,
            prefix: Some(prefix),
        }
    }

    pub fn with_config(mut self, config: KeySpaceConfig) -> Self {
        self.config = config;
        self
    }
}

pub fn default_cells_per_mutex() -> usize {
    1
}

#[cfg(test)]
mod adapter_metrics_tests {
    use super::*;

    fn stat_value(families: &[prometheus::proto::MetricFamily], stat: &str) -> Option<f64> {
        families
            .iter()
            .find(|family| family.name() == "tidehunter_adapter")?
            .get_metric()
            .iter()
            .find(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == "stat" && label.value() == stat)
            })
            .and_then(|metric| metric.get_gauge().as_ref())
            .map(|gauge| gauge.value())
    }

    #[test]
    fn failed_publication_and_stop_remain_explicitly_unavailable() {
        use std::sync::atomic::{AtomicBool, AtomicU64};
        use std::sync::{Arc, Mutex};

        let values = prometheus::IntGaugeVec::new(
            prometheus::Opts::new("tidehunter_adapter", "test adapter metrics"),
            &["stat"],
        )
        .unwrap();
        let collector = AdapterCollector {
            values: values.clone(),
            keyspace_cache: None,
            local_available: Arc::new(AtomicBool::new(true)),
            local_generation: Arc::new(AtomicU64::new(4)),
            local_last_success: Arc::new(Mutex::new(Some(std::time::Instant::now()))),
            local_stopped: Arc::new(AtomicBool::new(false)),
            metrics_lock: Arc::new(Mutex::new(())),
        };
        values.with_label_values(&["local_sample_available"]).set(1);
        values.with_label_values(&["read_calls"]).set(99);
        let native = prometheus::core::Collector::collect(&collector);
        assert_eq!(stat_value(&native, "local_sample_available"), Some(1.0));
        assert_eq!(stat_value(&native, "read_calls"), Some(99.0));
        {
            let _metrics = collector
                .metrics_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            collector.invalidate_locked();
        }
        let failed = prometheus::core::Collector::collect(&collector);
        assert_eq!(stat_value(&failed, "local_sample_available"), Some(0.0));
        assert_eq!(stat_value(&failed, "local_sample_generation"), Some(4.0));
        assert_eq!(stat_value(&failed, "read_calls"), None);

        collector.mark_stopped();
        let stopped = prometheus::core::Collector::collect(&collector);
        assert_eq!(stat_value(&stopped, "local_sample_available"), Some(0.0));
        assert_eq!(stat_value(&stopped, "local_sampler_stopped"), Some(1.0));
    }
}

#[cfg(test)]
mod campaign_config_tests {
    use super::*;

    #[test]
    fn auto_sharding_off_disables_the_threshold() {
        let mut config = Config::default();
        let default_threshold = config.auto_shard_threshold();
        apply_auto_sharding(&mut config, Some(false));
        assert_eq!(
            config.auto_shard_threshold(),
            usize::MAX,
            "TH_AUTOSHARD=false must disable auto-sharding; the enabled threshold was {default_threshold}"
        );
        apply_auto_sharding(&mut config, Some(true));
        assert_eq!(
            config.auto_shard_threshold(),
            default_threshold,
            "TH_AUTOSHARD=true must restore the default threshold"
        );
        apply_auto_sharding(&mut config, None);
        assert_eq!(
            config.auto_shard_threshold(),
            default_threshold,
            "an absent TH_AUTOSHARD must keep the default threshold"
        );
    }

    #[cfg(feature = "sharded-wal")]
    #[test]
    fn remote_tier_check_rejects_a_sync_writes_disagreement() {
        use tidehunter::distributed::durability::DurabilityTier;

        let mismatch = remote_durability_mismatch(DurabilityTier::Synchronous, Some(false))
            .expect("a false TH_SYNC_WRITES against the synchronous tier must fail");
        assert!(
            mismatch.contains("TH_SYNC_WRITES=false") && mismatch.contains("synchronous"),
            "the error must name the requested and configured tiers: {mismatch}"
        );
        assert!(
            remote_durability_mismatch(DurabilityTier::Native, Some(false)).is_none(),
            "a native document with TH_SYNC_WRITES=false must open"
        );
        assert!(
            remote_durability_mismatch(DurabilityTier::Synchronous, Some(true)).is_none(),
            "a synchronous document with TH_SYNC_WRITES=true must open"
        );
        assert!(
            remote_durability_mismatch(DurabilityTier::Synchronous, None).is_none(),
            "an absent TH_SYNC_WRITES must keep the document tier"
        );
    }
}

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
#[cfg(not(feature = "sharded-wal"))]
use tidehunter::compressed_batch::BatchCodec;
#[cfg(not(feature = "sharded-wal"))]
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

pub fn open(path: &Path, key_shape: KeyShape, metric_conf: &MetricConf) -> (Arc<Db>, RegistryID) {
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
    register_remote_metrics(&registry, &db, path);
    (db, registry_id)
}

#[cfg(feature = "sharded-wal")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ShardedWalConfig {
    root: std::path::PathBuf,
    databases: std::collections::BTreeMap<
        String,
        tidehunter::distributed::remote_db::RemoteDatabaseConfig,
    >,
}

#[cfg(feature = "sharded-wal")]
fn sharded_config() -> Option<ShardedWalConfig> {
    env::var_os("SUI_SHARDED_WAL_CONFIG").map(|path| {
        let bytes = std::fs::read(path).expect("read SUI_SHARDED_WAL_CONFIG");
        serde_json::from_slice(&bytes).expect("parse SUI_SHARDED_WAL_CONFIG")
    })
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
        let remote = config
            .databases
            .get(&name)
            .unwrap_or_else(|| panic!("missing exact sharded-wal namespace {name:?}"))
            .clone();
        let _ = (metric_conf, metrics);
        Db::open_remote(path, shape, remote).expect("open sharded-wal Sui database")
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

struct KeyspaceCacheGauges {
    hits: prometheus::IntGauge,
    misses: prometheus::IntGauge,
    inserts: prometheus::IntGauge,
    evictions: prometheus::IntGauge,
}

struct AdapterCollector {
    db: std::sync::Weak<Db>,
    values: prometheus::IntGaugeVec,
    keyspace_cache: Option<(prometheus::IntGaugeVec, Box<[KeyspaceCacheGauges]>)>,
    shard_reads: Option<prometheus::IntGaugeVec>,
    io: Option<tidehunter::distributed::remote_db::ReadIoMonitor>,
}
impl prometheus::core::Collector for AdapterCollector {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        let mut desc = self.values.desc();
        if let Some((keyspace_cache, _)) = &self.keyspace_cache {
            desc.extend(keyspace_cache.desc());
        }
        if let Some(shards) = &self.shard_reads {
            desc.extend(shards.desc());
        }
        desc
    }
    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        use std::sync::atomic::Ordering;
        let mut shard_sample_available = false;
        if let Some(db) = self.db.upgrade() {
            let set = |name: &str, value: u64| {
                self.values
                    .with_label_values(&[name])
                    .set(value.min(i64::MAX as u64) as i64);
            };
            set("read_calls", db.read_calls.load(Ordering::Relaxed));
            set("read_keys", db.read_keys.load(Ordering::Relaxed));
            set("scan_records", db.scan_records.load(Ordering::Relaxed));
            db.visit_timing_metrics(set);
            if let Some((cache, writes, rpcs)) = db.remote_counters() {
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
            if let Some(monitor) = &self.io {
                match monitor.sample() {
                    Ok(samples) => {
                        set("shard_sample_available", 1);
                        shard_sample_available = true;
                        if let Some(shards) = &self.shard_reads {
                            for (partition, stats) in &samples {
                                let partition = partition.to_string();
                                for (name, value) in [
                                    ("cache_hits", stats.cache_hits),
                                    ("cache_misses", stats.cache_misses),
                                    ("cache_inserts", stats.cache_inserts),
                                    ("cache_evictions", stats.cache_evictions),
                                    ("cache_bytes", stats.cache_bytes as u64),
                                    ("cache_budget", stats.cache_budget as u64),
                                    ("wal_reads", stats.wal_reads),
                                    ("wal_read_bytes", stats.wal_read_bytes),
                                    ("resident_read_attempts", stats.resident_read_attempts),
                                    ("resident_read_successes", stats.resident_read_successes),
                                    ("resident_read_fallbacks", stats.resident_read_fallbacks),
                                    ("resident_read_unsupported", stats.resident_read_unsupported),
                                    ("resident_read_partial", stats.resident_read_partial),
                                    ("async_read_submissions", stats.async_read_submissions),
                                    ("completed_reads", stats.completed_reads),
                                    ("errors", stats.errors),
                                    ("cancellations", stats.cancellations),
                                    ("inflight_requests", stats.inflight_requests as u64),
                                    ("inflight_bytes", stats.inflight_bytes as u64),
                                    ("inflight_reads", stats.inflight_reads as u64),
                                ] {
                                    shards
                                        .with_label_values(&[&partition, name])
                                        .set(value.min(i64::MAX as u64) as i64);
                                }
                                #[cfg(feature = "sharded-wal")]
                                {
                                    for (name, value) in [
                                        ("resident_read_deferred", stats.resident_read_deferred),
                                        (
                                            "authority_available",
                                            u64::from(stats.authority.is_some()),
                                        ),
                                        ("initiator_available", 0),
                                    ] {
                                        shards
                                            .with_label_values(&[&partition, name])
                                            .set(value.min(i64::MAX as u64) as i64);
                                    }
                                    if let Some(authority) = stats.authority {
                                        for (name, value) in [
                                            ("authority_resident_hits", authority.resident_hits),
                                            (
                                                "authority_resident_misses",
                                                authority.resident_misses,
                                            ),
                                            ("authority_contention", authority.contention),
                                            ("authority_history_loads", authority.history_loads),
                                            ("authority_entries", authority.entries as u64),
                                            ("authority_capacity", authority.capacity as u64),
                                            ("authority_table_bytes", authority.table_bytes as u64),
                                        ] {
                                            shards
                                                .with_label_values(&[&partition, name])
                                                .set(value.min(i64::MAX as u64) as i64);
                                        }
                                    }
                                }
                            }
                        }
                        set(
                            "shard_cache_hits",
                            samples.iter().map(|(_, s)| s.cache_hits).sum(),
                        );
                        set(
                            "shard_cache_misses",
                            samples.iter().map(|(_, s)| s.cache_misses).sum(),
                        );
                        set(
                            "shard_cache_bytes",
                            samples.iter().map(|(_, s)| s.cache_bytes as u64).sum(),
                        );
                        set(
                            "shard_wal_reads",
                            samples.iter().map(|(_, s)| s.wal_reads).sum(),
                        );
                        set(
                            "shard_wal_read_bytes",
                            samples.iter().map(|(_, s)| s.wal_read_bytes).sum(),
                        );
                        set(
                            "shard_async_read_submissions",
                            samples.iter().map(|(_, s)| s.async_read_submissions).sum(),
                        );
                        set(
                            "shard_read_errors",
                            samples.iter().map(|(_, s)| s.errors).sum(),
                        );
                    }
                    Err(error) => {
                        set("shard_sample_available", 0);
                        tracing::warn!(?error, "sharded WAL diagnostic sample unavailable");
                    }
                }
                #[cfg(feature = "sharded-wal")]
                match monitor.sample_resident_reads() {
                    Ok(samples) => {
                        set("resident_sample_available", 1);
                        if let Some(shards) = &self.shard_reads {
                            for (partition, stats) in samples {
                                let partition = partition.to_string();
                                for (name, value) in [
                                    ("initiator_available", 1),
                                    ("initiator_eligible_reads", stats.eligible_reads),
                                    (
                                        "initiator_message_read_requests",
                                        stats.message_read_requests,
                                    ),
                                    ("initiator_grant_acquisitions", stats.grant_acquisitions),
                                    ("initiator_rma_posts", stats.rma_posts),
                                    ("initiator_rma_completions", stats.rma_completions),
                                    ("initiator_rma_hits", stats.rma_hits),
                                    ("initiator_rma_errors", stats.rma_errors),
                                    ("initiator_fallback_missing", stats.fallback_missing),
                                    ("initiator_fallback_oversize", stats.fallback_oversize),
                                    ("initiator_fallback_capacity", stats.fallback_capacity),
                                    ("initiator_fallback_unsupported", stats.fallback_unsupported),
                                    ("initiator_fallback_stale", stats.fallback_stale),
                                    ("initiator_handles_live", stats.handles_live),
                                    (
                                        "initiator_destination_slots_live",
                                        stats.destination_slots_live,
                                    ),
                                ] {
                                    shards
                                        .with_label_values(&[&partition, name])
                                        .set(value.min(i64::MAX as u64) as i64);
                                }
                            }
                        }
                    }
                    Err(error) => {
                        set("resident_sample_available", 0);
                        shard_sample_available = false;
                        tracing::warn!(?error, "resident read diagnostic sample unavailable");
                    }
                }
            }
            if let (Some(counters), Some((_, gauges))) =
                (db.remote_keyspace_cache_counters(), &self.keyspace_cache)
            {
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
        }
        let mut families = self.values.collect();
        if let Some((keyspace_cache, _)) = &self.keyspace_cache {
            families.extend(keyspace_cache.collect());
        }
        if let Some(shards) = self.shard_reads.as_ref().filter(|_| shard_sample_available) {
            // A failed sample omits owner rows instead of exporting old counters.
            families.extend(shards.collect());
        }
        families
    }
}
fn register_remote_metrics(registry: &Registry, db: &Arc<Db>, path: &Path) {
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
    let io = db.remote_io_monitor();
    let shard_reads = io.as_ref().map(|_| {
        prometheus::IntGaugeVec::new(
            prometheus::Opts::new(
                "tidehunter_shard_read",
                "Per-owner cumulative read counters and current bounded-cache gauges",
            )
            .const_label("namespace", name.clone()),
            &["partition", "stat"],
        )
        .expect("shard read metric definition")
    });
    let keyspace_cache = io.as_ref().map(|_| {
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
    registry
        .register(Box::new(AdapterCollector {
            db: Arc::downgrade(db),
            values,
            keyspace_cache,
            shard_reads,
            io,
        }))
        .expect("register Tidehunter adapter counters");
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

#[cfg(not(feature = "sharded-wal"))]
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

    if parse_env_bool("TH_AUTOSHARD").unwrap_or(false) {
        config.with_index_auto_sharding();
    }

    config
}

#[cfg(not(debug_assertions))]
pub fn default_mutex_count() -> usize {
    parse_env_usize("TH_DEFAULT_MUTEX_COUNT", 1).unwrap_or(1024)
}

#[cfg(debug_assertions)]
pub fn default_mutex_count() -> usize {
    parse_env_usize("TH_DEFAULT_MUTEX_COUNT", 1).unwrap_or(16)
}

pub fn default_value_cache_size() -> usize {
    parse_env_usize("TH_DEFAULT_VALUE_CACHE_SIZE", 1).unwrap_or(1000)
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

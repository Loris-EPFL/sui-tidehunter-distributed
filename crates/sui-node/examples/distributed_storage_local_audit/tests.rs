// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! TEST ONLY: an unflushed merge WAL must not produce a partial successful audit.
use super::{TableAudit, audit, rpc_options};
use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use sui_consistent_store::{CfOptionsResolver, Encode};
use sui_rpc_store::schema::balance;
use sui_types::{TypeTag, base_types::SuiAddress};
use typed_store::rocksdb::{
    ColumnFamilyDescriptor, DB, DBRecoveryMode, Options, WriteBatch, WriteOptions,
};

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;
const FIXTURE_PATH: &str = "TIDEHUNTER_AUDIT_WAL_FIXTURE";
const CHILD_TEST: &str = "wal_merge_tests::wal_only_fixture_child";

#[cfg_attr(test, test)]
pub(super) fn rpc_merge_wal_recovers_exact_values_without_modifying_source() -> Result {
    let root = tempfile::tempdir()?;
    let path = create_wal_fixture(root.path())?;
    let before = source_files(&path)?;
    let result = audit(&path)?;
    let (key, value) = balance::delta(SuiAddress::from_bytes([7; 32])?, TypeTag::U64, 5, 3);
    let mut expected = TableAudit::default();
    expected.add(&key.encode()?, &value.encode()?);
    assert_eq!(result.tables.get("balance"), Some(&expected));
    assert_eq!(result.wal_recovery, "AbsoluteConsistency");
    assert_eq!(source_files(&path)?, before);
    Ok(())
}

#[cfg_attr(test, test)]
pub(super) fn strict_recovery_rejects_missing_rpc_merge_operator() -> Result {
    let root = tempfile::tempdir()?;
    let path = create_wal_fixture(root.path())?;
    let before = source_files(&path)?;
    let mut options = Options::default();
    options.set_wal_recovery_mode(DBRecoveryMode::AbsoluteConsistency);
    let families = DB::list_cf(&options, &path)?;
    match DB::open_cf_for_read_only(&options, &path, families, false) {
        Ok(_) => return Err("missing merge operator silently accepted the WAL".into()),
        Err(error) => assert!(
            error.to_string().contains("merge_operator"),
            "recovery must reject the missing merge operator: {error}"
        ),
    }
    assert_eq!(source_files(&path)?, before);
    Ok(())
}

fn create_wal_fixture(root: &Path) -> Result<PathBuf> {
    let path = root.join("rpc_store");
    let arguments = if cfg!(test) {
        vec!["--exact", CHILD_TEST, "--ignored", "--test-threads=1"]
    } else {
        vec!["--wal-fixture-child"]
    };
    let mut child = Command::new(std::env::current_exe()?)
        .args(arguments)
        .env(FIXTURE_PATH, &path)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait()? {
            if !status.success() {
                return Err(format!("WAL fixture child failed: {status}").into());
            }
            break;
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            return Err("WAL fixture child exceeded ten seconds".into());
        }
        thread::sleep(Duration::from_millis(10));
    }
    let files = source_files(&path)?;
    assert!(
        files
            .iter()
            .any(|(name, bytes)| name.ends_with(".log") && !bytes.is_empty())
    );
    assert!(files.keys().all(|name| !name.ends_with(".sst")));
    Ok(path)
}

fn source_files(path: &Path) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut files = BTreeMap::new();
    let mut bytes = 0_u64;
    for entry in fs::read_dir(path)?.take(33) {
        let entry = entry?;
        let metadata = entry.metadata()?;
        bytes += metadata.len();
        if files.len() >= 32 || !metadata.is_file() || bytes > (16 << 20) {
            return Err("WAL fixture exceeds bounded flat-file inventory".into());
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "fixture filename")?;
        files.insert(name, fs::read(entry.path())?);
    }
    Ok(files)
}

#[cfg_attr(test, test)]
#[cfg_attr(
    test,
    ignore = "invoked only by the bounded parent to avoid RocksDB shutdown flushing"
)]
pub(super) fn wal_only_fixture_child() -> Result {
    let path = std::env::var_os(FIXTURE_PATH).ok_or("missing WAL fixture path")?;
    let resolver = CfOptionsResolver::new(sui_rpc_store::default_rocksdb_config())?;
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    let mut balance_options = rpc_options("balance", &resolver)?;
    balance_options.set_write_buffer_size(8 << 20);
    balance_options.set_disable_auto_compactions(true);
    let db = DB::open_cf_descriptors(
        &options,
        path,
        [ColumnFamilyDescriptor::new("balance", balance_options)],
    )?;
    let cf = db
        .cf_handle("balance")
        .ok_or("missing fixture balance CF")?;
    let mut batch = WriteBatch::default();
    for (coin, address) in [(7, 0), (-2, 3)] {
        let (key, value) = balance::delta(
            SuiAddress::from_bytes([7; 32])?,
            TypeTag::U64,
            coin,
            address,
        );
        batch.merge_cf(&cf, key.encode()?, value.encode()?);
    }
    let mut writes = WriteOptions::default();
    writes.set_sync(true);
    db.write_opt(batch, &writes)?;
    // A normal Db drop can materialize its memtable. Exit only this owned child
    // after synchronizing the WAL, leaving recovery as the sole source of rows.
    std::process::exit(0)
}

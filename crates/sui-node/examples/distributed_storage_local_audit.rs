// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! TEST-ONLY stopped-store audit. Uses read-only RocksDB access; no Sui services.
use fastcrypto::hash::{Blake2b256, HashFunction};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    path::Path,
    sync::{Arc, atomic::AtomicU64},
};
use sui_consistent_store::{CfOptionsResolver, Decode};
use sui_rpc_store::schema;
use typed_store::rocksdb::{ColumnFamilyDescriptor, DB, DBRecoveryMode, IteratorMode, Options};

const MAX_ROWS: usize = 10_000_000;

#[path = "distributed_storage_local_audit/tests.rs"]
mod wal_merge_tests;

#[derive(Clone, Default, PartialEq, Debug, Serialize)]
struct TableAudit {
    records: u64,
    key_bytes: u64,
    value_bytes: u64,
    digest_xor: [u8; 32],
}

#[derive(Serialize)]
struct Audit {
    test_only: bool,
    wal_recovery: &'static str,
    schema: &'static str,
    tables: BTreeMap<String, TableAudit>,
    semantic_tables: BTreeMap<String, TableAudit>,
    sequence_digest_bijection: bool,
}

impl TableAudit {
    fn add(&mut self, key: &[u8], value: &[u8]) {
        self.records += 1;
        self.key_bytes += key.len() as u64;
        self.value_bytes += value.len() as u64;
        let mut hash = Blake2b256::new();
        hash.update((key.len() as u64).to_be_bytes());
        hash.update(key);
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value);
        let digest: [u8; 32] = hash.finalize().into();
        for (combined, byte) in self.digest_xor.iter_mut().zip(digest) {
            *combined ^= byte;
        }
    }
}

fn rpc_options(name: &str, resolver: &CfOptionsResolver) -> Result<Options, Box<dyn Error>> {
    // Match RpcStoreSchema::open, including all four merge operators. Read-only
    // access cannot compact. We additionally reject any persisted pruning floor.
    Ok(match name {
        "epochs" => schema::epochs::options(resolver),
        "checkpoint_summary" => schema::checkpoint_summary::options(resolver),
        "checkpoint_contents" => schema::checkpoint_contents::options(resolver),
        "checkpoint_seq_by_digest" => schema::checkpoint_seq_by_digest::options(resolver),
        "transactions" => schema::transactions::options(resolver),
        "tx_seq_by_digest" => schema::tx_seq_by_digest::options(resolver),
        "tx_metadata_by_seq" => schema::tx_metadata_by_seq::options(resolver),
        "effects" => schema::effects::options(resolver),
        "events" => schema::events::options(resolver),
        "objects" => schema::objects::options(resolver),
        "object_version_by_checkpoint" => schema::object_version_by_checkpoint::options(resolver),
        "object_by_owner" => schema::object_by_owner::options(resolver),
        "object_by_type" => schema::object_by_type::options(resolver),
        "balance" => schema::balance::options(resolver),
        "package_versions" => schema::package_versions::options(resolver),
        "transaction_bitmap" => {
            schema::transaction_bitmap::options(resolver, Arc::new(AtomicU64::new(0)))
        }
        "event_bitmap" => schema::event_bitmap::options(resolver, Arc::new(AtomicU64::new(0))),
        "pruning_watermark" => schema::pruning_watermark::options(resolver),
        "default" | "__restore" | "__watermark" | "__chain_id" => resolver.options(name),
        _ => return Err(format!("unsupported RPC column family: {name}").into()),
    })
}

fn audit(path: &Path) -> Result<Audit, Box<dyn Error>> {
    let rpc = path.file_name().is_some_and(|name| name == "rpc_store");
    let indexes = path.file_name().is_some_and(|name| name == "indexes");
    let mut options = Options::default();
    // PointInTimeRecovery can silently stop at a missing merge operator and
    // return a partial WAL view. Any WAL recovery error must reject this audit.
    options.set_wal_recovery_mode(DBRecoveryMode::AbsoluteConsistency);
    let families = DB::list_cf(&options, path)?;
    if families.is_empty() || families.len() > 256 {
        return Err("audit requires 1..256 column families".into());
    }
    let resolver = CfOptionsResolver::new(sui_rpc_store::default_rocksdb_config())?;
    let descriptors = families
        .iter()
        .map(|name| {
            Ok(ColumnFamilyDescriptor::new(
                name,
                if rpc {
                    rpc_options(name, &resolver)?
                } else {
                    Options::default()
                },
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let database = DB::open_cf_descriptors_read_only(&options, path, descriptors, false)?;
    let mut sequence_to_digest = BTreeMap::new();
    if indexes {
        let handle = database
            .cf_handle("transaction_order")
            .ok_or("missing transaction order")?;
        let mut digests = BTreeSet::new();
        for (index, row) in database
            .iterator_cf(&handle, IteratorMode::Start)
            .take(MAX_ROWS + 1)
            .enumerate()
        {
            if index == MAX_ROWS {
                return Err("sequence audit row budget exceeded".into());
            }
            let (key, value) = row?;
            let sequence = u64::from_be_bytes(key.as_ref().try_into()?);
            let digest: sui_types::digests::TransactionDigest = bcs::from_bytes(&value)?;
            if !digests.insert(digest) || sequence_to_digest.insert(sequence, digest).is_some() {
                return Err("transaction order is not a sequence/digest bijection".into());
            }
        }
    }
    let mut tables = BTreeMap::new();
    let mut semantic_tables = BTreeMap::new();
    let mut inverse_rows = 0;
    for family in families {
        let handle = database.cf_handle(&family).ok_or("column family missing")?;
        let mut table = TableAudit::default();
        let mut semantic = TableAudit::default();
        for (index, row) in database
            .iterator_cf(&handle, IteratorMode::Start)
            .take(MAX_ROWS + 1)
            .enumerate()
        {
            if index == MAX_ROWS {
                return Err("local store audit row budget exceeded".into());
            }
            let (key, value) = row?;
            table.add(&key, &value);
            if rpc && family == "pruning_watermark" {
                let mut encoded = value.as_ref();
                let watermark =
                    schema::pruning_watermark::Value::decode(&mut encoded)?.into_inner();
                if !encoded.is_empty() || watermark.tx_seq_lo != 0 || watermark.checkpoint_lo != 0 {
                    return Err(
                        "audit requires unpruned RPC fixture with zero pruning floors".into(),
                    );
                }
            }
            if indexes {
                match family.as_str() {
                    "transaction_order" => semantic.add(&value, &[]),
                    "transactions_seq" => {
                        // typed-store keys use fixed-width big-endian bincode;
                        // digest serde bytes has an eight-byte length prefix.
                        if key.len() != 40 || key[..8] != 32_u64.to_be_bytes() {
                            return Err("unsupported transaction digest key encoding".into());
                        }
                        let sequence: u64 = bcs::from_bytes(&value)?;
                        let digest = sequence_to_digest
                            .get(&sequence)
                            .ok_or("inverse sequence missing")?;
                        let canonical = bcs::to_bytes(digest)?;
                        if canonical[1..] != key[8..] {
                            return Err("inverse transaction digest mismatch".into());
                        }
                        inverse_rows += 1;
                        semantic.add(&canonical, &[]);
                    }
                    "transactions_from_addr"
                    | "transactions_to_addr"
                    | "transactions_by_input_object_id"
                    | "transactions_by_mutated_object_id"
                    | "transactions_by_move_function" => {
                        if key.len() < 8 {
                            return Err("invalid sequence index key".into());
                        }
                        let prefix_len = key.len() - 8;
                        let sequence = u64::from_be_bytes(key[prefix_len..].try_into()?);
                        let digest = sequence_to_digest
                            .get(&sequence)
                            .ok_or("index sequence missing")?;
                        if bcs::to_bytes(digest)?.as_slice() != value.as_ref() {
                            return Err("index transaction digest mismatch".into());
                        }
                        semantic.add(&key[..prefix_len], &value);
                    }
                    name if name.starts_with("event_") => {
                        return Err("nonempty event index semantic audit is not implemented".into());
                    }
                    _ => semantic.add(&key, &value),
                }
            } else {
                semantic.add(&key, &value);
            }
        }
        semantic_tables.insert(family.clone(), semantic);
        tables.insert(family, table);
    }
    if indexes && inverse_rows != sequence_to_digest.len() {
        return Err("transaction sequence inverse does not cover all ordered transactions".into());
    }
    Ok(Audit {
        test_only: true,
        wal_recovery: "AbsoluteConsistency",
        schema: if rpc {
            "RpcStoreSchema; zero pruning floors verified"
        } else if indexes {
            "IndexStoreTables; sequence/digest bijection verified"
        } else {
            "raw RocksDB"
        },
        tables,
        semantic_tables,
        sequence_digest_bijection: indexes,
    })
}

fn main() -> Result<(), Box<dyn Error>> {
    let arguments: Vec<_> = std::env::args_os().take(3).collect();
    let [_, path] = arguments.as_slice() else {
        return Err("usage: distributed_storage_local_audit STOPPED_ROCKSDB_PATH".into());
    };
    if path == "--wal-fixture-child" {
        return wal_merge_tests::wal_only_fixture_child();
    }
    if path == "--self-test" {
        wal_merge_tests::rpc_merge_wal_recovers_exact_values_without_modifying_source()?;
        wal_merge_tests::strict_recovery_rejects_missing_rpc_merge_operator()?;
        sequence_tests::local_sequence_order_changes_do_not_change_semantic_rows()?;
        sequence_tests::wrong_inverse_digest_is_rejected()?;
        println!("Four stopped-store audit regressions passed");
        return Ok(());
    }
    println!("{}", serde_json::to_string(&audit(Path::new(path))?)?);
    Ok(())
}

mod sequence_tests {
    use super::*;

    fn fixture(path: &Path, reverse: bool, bad_inverse: bool) -> Result<(), Box<dyn Error>> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf(
            &options,
            path,
            [
                "transaction_order",
                "transactions_seq",
                "transactions_from_addr",
            ],
        )?;
        for offset in 0..2_u64 {
            let sequence = if reverse { 1 - offset } else { offset };
            let digest = sui_types::digests::TransactionDigest::new([offset as u8; 32]);
            let value = bcs::to_bytes(&digest)?;
            let key = [32_u64.to_be_bytes().as_slice(), &value[1..]].concat();
            db.put_cf(
                &db.cf_handle("transaction_order").ok_or("order CF")?,
                sequence.to_be_bytes(),
                &value,
            )?;
            db.put_cf(
                &db.cf_handle("transactions_seq").ok_or("inverse CF")?,
                key,
                bcs::to_bytes(&(if bad_inverse { 1 - sequence } else { sequence }))?,
            )?;
            let address_key = [[7_u8; 32].as_slice(), &sequence.to_be_bytes()].concat();
            db.put_cf(
                &db.cf_handle("transactions_from_addr").ok_or("address CF")?,
                address_key,
                value,
            )?;
        }
        Ok(())
    }

    #[cfg_attr(test, test)]
    pub(super) fn local_sequence_order_changes_do_not_change_semantic_rows()
    -> Result<(), Box<dyn Error>> {
        let left = tempfile::tempdir()?;
        let right = tempfile::tempdir()?;
        fixture(&left.path().join("indexes"), false, false)?;
        fixture(&right.path().join("indexes"), true, false)?;
        let a = audit(&left.path().join("indexes"))?;
        let b = audit(&right.path().join("indexes"))?;
        assert_ne!(a.tables, b.tables);
        assert_eq!(a.semantic_tables, b.semantic_tables);
        assert!(a.sequence_digest_bijection && b.sequence_digest_bijection);
        Ok(())
    }

    #[cfg_attr(test, test)]
    pub(super) fn wrong_inverse_digest_is_rejected() -> Result<(), Box<dyn Error>> {
        let root = tempfile::tempdir()?;
        fixture(&root.path().join("indexes"), false, true)?;
        assert!(audit(&root.path().join("indexes")).is_err());
        Ok(())
    }
}

//! Storage component tests: the storage engine and the persist layer under
//! it, driven in process. Durability, recovery, restart equivalence, the
//! data directory lock, backups and name validation, one module per subject.

mod harness;

mod backup_restore;
mod backup_safety;
mod crash_recovery;
mod data_dir_lock;
mod data_dir_loss;
mod name_validation;
mod online_checkpoint;
mod persist_crash;
mod persist_roundtrip;
mod restart_equivalence;
mod storage_engine;
mod wal_transaction_recovery;

//! Write-Ahead Log (WAL) for `InputLayer`
//!
//! Provides O(1) append-only persistence for database writes, with periodic
//! compaction to Parquet for query efficiency.
//!
//! ## Architecture
//!
//! ```text
//! Insert/Delete -> WAL (append, O(1.clone())) -> Periodic compaction -> Parquet
//!                      |
//!                      v
//!                 Recovery on startup (replay WAL)
//! ```
//!
//! ## WAL Entry Format
//!
//! Each entry is a JSON line (for simplicity and debuggability):
//! ```json
//! {"op":"insert","relation":"edge","tuples":[[1,2],[3,4]],"ts":1234567890}
//! {"op":"delete","relation":"edge","tuples":[[1,2]],"ts":1234567891}
//! ```

use serde::{Deserialize, Serialize};

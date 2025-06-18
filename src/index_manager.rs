//! Index Manager for Vector Similarity Search
//!
//! Manages HNSW and other indexes for knowledge graphs. Follows the same
//! pattern as `DerivedRelationsManager` for per-KG isolation and cascade
//! invalidation.
//!
//! ## Architecture
//!
//! ```text
//! Base Relations (DDComputation)
//!        |
//!        |--- documents(id, title, embedding)
//!        `--- ...
//!              |
//!              ▼
//!     IndexManager
//!        |
//!        |--- RegisteredIndex (metadata)
//!        |         |
//!        |         ▼
//!        `--- MaterializedIndex (HNSW structure + validity)
//! ```
//!
//! ## Key Concepts
//!
//! - RegisteredIndex: Index definition (relation, column, config)
//! - MaterializedIndex: Built index structure with validity tracking
//! - Dependency Tracking: Maps base relations -> dependent indexes
//! - Cascade Invalidation: Base updates invalidate dependent indexes

use std::collections::{HashMap, HashSet};

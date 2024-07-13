//! Query Cache Module
//!
//! Provides caching for:
//! - Compiled queries (IR nodes)
//! - Query results
//!
//! ## Design
//!
//! Uses LRU (Least Recently Used) eviction with configurable size limits.
//! Cache entries have TTL (time-to-live) for result caching.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};


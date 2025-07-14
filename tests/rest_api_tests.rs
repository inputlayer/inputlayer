//! REST API endpoint tests (tower test utilities, no server needed).

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use inputlayer::config::HttpConfig;
use inputlayer::protocol::rest::create_router;
use inputlayer::protocol::Handler;
use inputlayer::{Config, StorageEngine};
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

// Note: We keep Arc in scope for the Handler but don't wrap the Router in Arc


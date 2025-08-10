//! Rules Handlers
//!
//! Endpoints for persistent rule (policy) management.

use std::sync::Arc;

use axum::{extract::Path, Extension, Json};
use serde::Serialize;
use utoipa::ToSchema;

use crate::protocol::rest::dto::ApiResponse;
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;

/// Rule information
#[derive(Debug, Serialize, ToSchema)]

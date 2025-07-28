//! Data Handlers
//!
//! Endpoints for data manipulation (insert, delete).

use std::sync::Arc;

use axum::{extract::Path, Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::protocol::rest::dto::ApiResponse;
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;
use crate::value::{Tuple, Value};

/// Convert a JSON value to a storage Value

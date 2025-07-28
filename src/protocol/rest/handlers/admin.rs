//! Admin Handlers
//!
//! Health check and statistics endpoints.

use std::sync::Arc;

use axum::{Extension, Json};

use crate::protocol::rest::dto::{ApiResponse, HealthDto, StatsDto};
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;

/// Health check endpoint
#[utoipa::path(
    get,
    path = "/health",
    tag = "admin",
    responses(
        (status = 200, description = "Server is healthy", body = ApiResponse<HealthDto>),
    )
)]

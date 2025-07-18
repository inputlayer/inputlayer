//! REST API Module
//!
//! Provides HTTP REST API endpoints via Axum for the `InputLayer` GUI and external clients.
//! This is the primary API interface with `OpenAPI` documentation available at `/api/docs`.

pub mod dto;
pub mod error;
pub mod handlers;
pub mod openapi;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    routing::{delete, get, post},
    Extension, Router,
};

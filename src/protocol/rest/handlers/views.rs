//! Views Handlers
//!
//! Endpoints for view management operations.

use std::sync::Arc;

use axum::{
    extract::{Path, Query},
    Extension, Json,
};

use super::wire_value_to_json;
use crate::protocol::rest::dto::{
    ApiResponse, CreateViewRequest, RelationDataDto, RelationDataQuery, ViewDto, ViewListDto,
};

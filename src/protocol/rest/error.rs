//! REST API Error Types
//!
//! Provides error types and conversions for the REST API.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

/// API error response
#[derive(Debug, Serialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
}

impl ApiError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new("NOT_FOUND", message)
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new("BAD_REQUEST", message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new("INTERNAL_ERROR", message)
    }
}

/// REST API error that can be returned from handlers
#[derive(Debug)]
pub struct RestError {
    pub status: StatusCode,
    pub error: ApiError,
}

impl RestError {
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error: ApiError::not_found(message),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error: ApiError::bad_request(message),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: ApiError::internal(message),
        }
    }
}

impl IntoResponse for RestError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({
            "success": false,
            "error": self.error
        }));
        (self.status, body).into_response()
    }
}

// Conversions from domain errors
impl From<String> for RestError {
    fn from(err: String) -> Self {
        RestError::internal(err)
    }
}

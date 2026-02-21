//! HTTP API Error Types
//!
//! Provides error types and conversions for the HTTP handlers.

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

/// HTTP API error that can be returned from handlers
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

    pub fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            error: ApiError::new("SERVICE_UNAVAILABLE", message),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_error_not_found() {
        let err = ApiError::not_found("item missing");
        assert_eq!(err.code, "NOT_FOUND");
        assert_eq!(err.message, "item missing");
    }

    #[test]
    fn test_api_error_bad_request() {
        let err = ApiError::bad_request("invalid input");
        assert_eq!(err.code, "BAD_REQUEST");
        assert_eq!(err.message, "invalid input");
    }

    #[test]
    fn test_api_error_internal() {
        let err = ApiError::internal("server error");
        assert_eq!(err.code, "INTERNAL_ERROR");
        assert_eq!(err.message, "server error");
    }

    #[test]
    fn test_api_error_custom_code() {
        let err = ApiError::new("CUSTOM", "custom message");
        assert_eq!(err.code, "CUSTOM");
        assert_eq!(err.message, "custom message");
    }

    #[test]
    fn test_rest_error_not_found_status() {
        let err = RestError::not_found("not here");
        assert_eq!(err.status, StatusCode::NOT_FOUND);
        assert_eq!(err.error.code, "NOT_FOUND");
    }

    #[test]
    fn test_rest_error_bad_request_status() {
        let err = RestError::bad_request("bad");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_rest_error_internal_status() {
        let err = RestError::internal("oops");
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_rest_error_from_string() {
        let err: RestError = "something went wrong".to_string().into();
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.error.message, "something went wrong");
    }

    #[test]
    fn test_api_error_serialization() {
        let err = ApiError::not_found("test");
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("\"code\":\"NOT_FOUND\""));
        assert!(json.contains("\"message\":\"test\""));
    }

    #[test]
    fn test_rest_error_into_response() {
        let err = RestError::not_found("gone");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

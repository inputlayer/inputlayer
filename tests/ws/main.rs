//! WebSocket component tests: an in-process server on a loopback port and a
//! raw `/ws` client. Sessions, pipelining, cancellation, delivery,
//! `expect_revision`, notification cursors, credential revocation and
//! authentication hardening, one module per subject.

mod harness;

mod auth_hardening;
mod credential_revocation;
mod notification_cursor;
mod ws_cancel;
mod ws_delivery;
mod ws_expect_revision;
mod ws_hygiene;
mod ws_pipeline;
mod ws_session;

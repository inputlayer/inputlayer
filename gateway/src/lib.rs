//! InputLayer Gateway library: the verify pipeline behind the
//! `inputlayer-gateway` binary.
//!
//! Split out as a library so integration tests exercise the exact code the
//! binary runs. The pipeline is: extract (model, ontology-bound) -> validate
//! quotes -> map to IQL -> deploy + insert into an ephemeral KG -> report.

pub mod mapper;
pub mod model;
pub mod ontology;
pub mod pipeline;

//! Recompute adapter: every observation is a fresh query against the current
//! snapshot through the normal program path.
//!
//! The `maintained` adapter is the same adapter on an engine whose persistent
//! rules are incrementally maintained views (`engine.views = "maintained"`),
//! so the reference, recompute and maintained answers are compared at every
//! checkpoint. It joins the standard set when `INPUTLAYER_ORACLE_VIEWS` is
//! `maintained`.

use inputlayer_testkit::Mode;

use crate::adapter::Adapter;
use crate::engine::{result_rows, EngineHost};
use crate::model::{AdapterError, Observation, Outcome, Revision};

pub struct RecomputeAdapter {
    host: EngineHost,
    name: &'static str,
}

impl RecomputeAdapter {
    pub fn open() -> Result<Self, AdapterError> {
        Ok(Self {
            host: EngineHost::open()?,
            name: "recompute",
        })
    }

    /// The `maintained` adapter.
    pub fn open_maintained() -> Result<Self, AdapterError> {
        Ok(Self {
            host: EngineHost::open_in(Mode::Maintained)?,
            name: "maintained",
        })
    }
}

impl Adapter for RecomputeAdapter {
    fn name(&self) -> &'static str {
        self.name
    }

    fn execute(&mut self, statement: &str, _revision: Revision) -> Result<Outcome, AdapterError> {
        Ok(self.host.execute(statement))
    }

    fn restart(&mut self, _revision: Revision) -> Result<(), AdapterError> {
        self.host.restart()
    }

    fn observe(&mut self, query: &str, _revision: Revision) -> Result<Observation, AdapterError> {
        // Statements apply synchronously, so the snapshot is at `revision`.
        let result = self.host.run(query).map_err(AdapterError::Failed)?;
        Ok(Observation::from_rows(result_rows(result)?))
    }
}

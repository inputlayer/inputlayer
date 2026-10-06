//! A real engine instance for the engine-backed adapters.
//!
//! Each adapter gets its own data directory and `Handler`, so adapters cannot
//! observe each other's state. Restart shuts the handler down and reopens the
//! same directory, exercising WAL/parquet recovery of facts and rules.
//!
//! An engine answers reads of persistent rules in a views [`Mode`]
//! (`engine.views`): `recompute` re-derives them on every read (today's
//! engine), `maintained` keeps them as incrementally maintained views.
//! [`VIEWS_ENV`] selects the mode of the `maintained` adapter's run.

use std::sync::Arc;

use inputlayer::config::{DurabilityMode, ViewsMode};
use inputlayer::protocol::rest::handlers::wire_value_to_json;
use inputlayer::protocol::wire::QueryResult;
use inputlayer::protocol::Handler;
use inputlayer::Config;
use inputlayer_testkit::Mode;
use tempfile::TempDir;

use crate::model::{AdapterError, Cell, Outcome, Row};

/// Knowledge graph every history runs in.
pub const KG: &str = "oracle";

/// Environment variable selecting the views mode the oracle checks
/// (`recompute`, the default, or `maintained`).
pub const VIEWS_ENV: &str = "INPUTLAYER_ORACLE_VIEWS";

/// The views mode [`VIEWS_ENV`] selects.
///
/// # Panics
/// On a value other than `recompute` or `maintained`.
pub fn views_mode() -> Mode {
    Mode::from_env(VIEWS_ENV)
}

pub struct EngineHost {
    runtime: tokio::runtime::Runtime,
    dir: TempDir,
    views: Mode,
    /// `None` only between shutting down and reopening during a restart.
    handler: Option<Arc<Handler>>,
}

impl EngineHost {
    pub fn open() -> Result<Self, AdapterError> {
        Self::open_in(Mode::Recompute)
    }

    /// An engine answering reads of persistent rules in `views` mode;
    /// fails when this engine cannot run that mode.
    pub fn open_in(views: Mode) -> Result<Self, AdapterError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| AdapterError::Failed(format!("tokio runtime: {e}")))?;
        let dir = scratch_dir().map_err(|e| AdapterError::Failed(format!("temp dir: {e}")))?;
        let handler = Some(start(&runtime, &dir, views)?);
        Ok(Self {
            runtime,
            dir,
            views,
            handler,
        })
    }

    pub fn handler(&self) -> &Arc<Handler> {
        self.handler
            .as_ref()
            .expect("engine handler is only absent inside restart")
    }

    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    /// Run one statement through the normal program path.
    pub fn run(&self, program: &str) -> Result<QueryResult, String> {
        self.block_on(self.handler().execute_program(
            None,
            Some(KG.to_string()),
            program.to_string(),
            None,
        ))
        .map_err(|error| error.to_string())
    }

    /// Run a state-changing statement; engine errors are rejections.
    pub fn execute(&self, statement: &str) -> Outcome {
        match self.run(statement) {
            Ok(_) => Outcome::Applied,
            Err(message) => Outcome::Rejected(message),
        }
    }

    /// Shut down cleanly and reopen the same data directory.
    pub fn restart(&mut self) -> Result<(), AdapterError> {
        let old = self
            .handler
            .take()
            .expect("engine handler is only absent inside restart");
        old.shutdown();
        if Arc::strong_count(&old) != 1 {
            return Err(AdapterError::Failed(
                "handler still shared at restart; the old instance would keep running".into(),
            ));
        }
        drop(old);
        self.handler = Some(start(&self.runtime, &self.dir, self.views)?);
        Ok(())
    }
}

/// A data directory on tmpfs where available: every commit fsyncs, which
/// costs tens of milliseconds per statement on a busy disk and would make
/// randomized histories too slow to run on every test pass.
fn scratch_dir() -> std::io::Result<TempDir> {
    let shm = std::path::Path::new("/dev/shm");
    if shm.is_dir() {
        if let Ok(dir) = TempDir::new_in(shm) {
            return Ok(dir);
        }
    }
    TempDir::new()
}

/// The engine's config in `views` mode.
fn config(dir: &TempDir, views: Mode) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = dir.path().join("data");
    config.storage.performance.num_threads = 2;
    config.storage.persist.durability_mode = DurabilityMode::Immediate;
    config.http.gui.enabled = false;
    if views == Mode::Maintained {
        config.engine.views = ViewsMode::Maintained;
    }
    config
}

fn start(
    runtime: &tokio::runtime::Runtime,
    dir: &TempDir,
    views: Mode,
) -> Result<Arc<Handler>, AdapterError> {
    let _guard = runtime.enter();
    let handler = Arc::new(Handler::from_config(config(dir, views)).map_err(AdapterError::Failed)?);
    {
        let storage = handler.get_storage();
        // Present after a restart; created on first open.
        if !storage.list_knowledge_graphs().iter().any(|kg| kg == KG) {
            storage
                .create_knowledge_graph(KG)
                .map_err(|e| AdapterError::Failed(format!("create kg '{KG}': {e}")))?;
        }
    }
    Ok(handler)
}

/// Normalize a query result's rows.
pub fn result_rows(result: QueryResult) -> Result<Vec<Row>, AdapterError> {
    if result.truncated {
        return Err(AdapterError::Failed(format!(
            "result truncated at {} of {} rows",
            result.rows.len(),
            result.total_count
        )));
    }
    Ok(result
        .rows
        .into_iter()
        .map(|row| {
            row.values
                .into_iter()
                .map(|v| Cell::from_json(&wire_value_to_json(v)))
                .collect()
        })
        .collect())
}

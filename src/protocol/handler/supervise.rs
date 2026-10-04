//! Running a request's computation under its deadline and cancellation.
//!
//! One [`RequestControl`] spans the whole request: waiting for a compute
//! permit, waiting on the blocking pool, and computing. A request stopped
//! while it waits never starts; one stopped while it computes is abandoned at
//! once and its computation exits at its next cooperative check, releasing
//! the permit. A request that began committing is not interrupted: the wait
//! continues until it finishes, and its result is what it committed.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{info, warn};

use super::ProgramError;
use crate::execution::{RequestControl, Stop};
use crate::protocol::wire::ErrorCode;
use crate::statement::Statement;

/// Longest wait for a compute permit, whatever the request's deadline: past
/// it the server is overloaded and says so rather than queueing further.
const MAX_ADMISSION_WAIT: Duration = Duration::from_secs(30);

/// The wire code of a request stopped for `stop`.
pub(crate) fn stop_code(stop: Stop) -> ErrorCode {
    match stop {
        Stop::Deadline => ErrorCode::DeadlineExceeded,
        Stop::Cancelled => ErrorCode::Cancelled,
        Stop::MemoryExhausted | Stop::ServerMemoryExhausted => ErrorCode::ResourceExhausted,
    }
}

/// The error of a request stopped for `stop`.
pub(crate) fn stop_error(stop: Stop) -> ProgramError {
    ProgramError {
        message: stop.message().to_string(),
        code: Some(stop_code(stop)),
    }
}

/// Run `job` on the blocking pool under `control`, holding one of
/// `permits` while it computes. `job` sees `control` as the thread's request
/// control, so the evaluator's cooperative checks and the commit boundary
/// observe its deadline and cancellation.
pub(super) async fn run_blocking<T, F>(
    permits: &Arc<Semaphore>,
    control: &Arc<RequestControl>,
    job: F,
) -> Result<T, ProgramError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, ProgramError> + Send + 'static,
{
    let permit = admit(permits, control).await?;
    let job_control = Arc::clone(control);
    let task = tokio::task::spawn_blocking(move || {
        // Stopped while queued on the blocking pool: never start.
        if let Some(stop) = job_control.stopped() {
            drop(permit);
            return Err(stop_error(stop));
        }
        let _scope = ControlScope::enter(job_control);
        let result = job();
        drop(permit);
        result
    });
    supervise(control, task).await
}

/// The thread's request control for one job; cleared on drop, panics
/// included, so a pooled thread never carries a finished request's control.
struct ControlScope;

impl ControlScope {
    fn enter(control: Arc<RequestControl>) -> Self {
        crate::code_generator::set_request_control(Some(control));
        Self
    }
}

impl Drop for ControlScope {
    fn drop(&mut self) {
        crate::code_generator::set_request_control(None);
    }
}

/// Gate the next statement of the running program: a stopped request runs
/// no further statement, and a command changing durable state first enters
/// the commit, so once it starts it is not interrupted.
pub(super) fn statement_gate(statement: &Statement) -> Result<(), Stop> {
    let Some(control) = crate::code_generator::current_request_control() else {
        return Ok(());
    };
    match statement {
        Statement::Meta(command) if command.changes_durable_state() => control.begin_commit(),
        _ => control.stopped().map_or(Ok(()), Err),
    }
}

/// Wait for a compute permit until `control` stops the request or the
/// server is overloaded.
async fn admit(
    permits: &Arc<Semaphore>,
    control: &RequestControl,
) -> Result<OwnedSemaphorePermit, ProgramError> {
    let started = Instant::now();
    let acquire = tokio::time::timeout(MAX_ADMISSION_WAIT, Arc::clone(permits).acquire_owned());
    let permit = tokio::select! {
        biased;
        acquired = acquire => match acquired {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                return Err("Query semaphore closed (server shutting down)".to_string().into())
            }
            Err(_) => {
                return Err(format!(
                    "Server overloaded: query queue full (timed out after {}s)",
                    MAX_ADMISSION_WAIT.as_secs()
                )
                .into())
            }
        },
        stop = control.interrupted() => {
            // Nothing has started, so nothing can be committing.
            let stop = stop.unwrap_or(Stop::Cancelled);
            info!(queued_ms = started.elapsed().as_millis() as u64, ?stop, "query_stopped_queued");
            return Err(stop_error(stop));
        }
    };
    let queued_ms = started.elapsed().as_millis() as u64;
    if queued_ms > 0 {
        info!(queued_ms, "query_semaphore_wait");
    }
    Ok(permit)
}

/// Await `task` under `control`; see the module docs.
async fn supervise<T>(
    control: &RequestControl,
    task: impl Future<Output = Result<Result<T, ProgramError>, tokio::task::JoinError>>,
) -> Result<T, ProgramError> {
    tokio::pin!(task);
    let joined = tokio::select! {
        joined = &mut task => joined,
        stop = control.interrupted() => match stop {
            Some(stop) => {
                warn!(?stop, "query_stopped_running");
                return Err(stop_error(stop));
            }
            // Committing or finished: not interruptible, wait it out.
            None => task.await,
        },
    };
    match joined {
        Ok(result) => {
            // A stop that won the race discards the result: the request
            // committed nothing, so reporting success would be false.
            control.finish().map_err(stop_error)?;
            result
        }
        Err(e) if control.is_committing() => {
            tracing::error!(error = %e, "query_task_panicked_while_committing");
            Err(ProgramError {
                message: "The request failed after it began committing; its changes may or \
                          may not be applied. Read the state back before retrying."
                    .to_string(),
                code: Some(ErrorCode::OutcomeUnknown),
            })
        }
        Err(e) => {
            tracing::error!(error = %e, "query_task_panicked");
            Err("Internal query execution error".to_string().into())
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn permits(n: usize) -> Arc<Semaphore> {
        Arc::new(Semaphore::new(n))
    }

    #[tokio::test]
    async fn a_request_stopped_while_queued_never_runs() {
        let permits = permits(1);
        let held = Arc::clone(&permits).acquire_owned().await.unwrap();
        let control = RequestControl::with_timeout(Some(Duration::from_millis(30)));
        let ran = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&ran);
        let result = run_blocking(&permits, &control, move || {
            flag.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await;
        assert_eq!(result.unwrap_err().code, Some(ErrorCode::DeadlineExceeded));
        drop(held);
        // The permit freed after the deadline must not start the job.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !ran.load(Ordering::SeqCst),
            "queued request ran after its deadline"
        );
    }

    #[tokio::test]
    async fn a_running_request_is_abandoned_and_its_computation_exits() {
        let permits = permits(1);
        let control = RequestControl::new(None);
        let exited = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&exited);
        let canceller = Arc::clone(&control);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            canceller.cancel();
        });
        let result = run_blocking(&permits, &control, move || {
            // A computation polling its cooperative check.
            while !crate::code_generator::current_request_control().is_some_and(|c| c.is_stopped())
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            flag.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await;
        assert_eq!(result.unwrap_err().code, Some(ErrorCode::Cancelled));
        // The permit comes back once the computation notices the stop.
        let permit = tokio::time::timeout(Duration::from_secs(5), permits.acquire())
            .await
            .unwrap();
        assert!(permit.is_ok());
        assert!(exited.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_committing_request_is_awaited_and_reports_its_result() {
        let permits = permits(1);
        let control = RequestControl::with_timeout(Some(Duration::from_millis(20)));
        let result = run_blocking(&permits, &control, move || {
            let control = crate::code_generator::current_request_control().unwrap();
            control.begin_commit().unwrap();
            // The deadline passes mid-commit.
            std::thread::sleep(Duration::from_millis(80));
            Ok("committed")
        })
        .await;
        assert_eq!(result.unwrap(), "committed");
        assert!(control.is_committing());
    }

    #[tokio::test]
    async fn a_panic_after_the_commit_began_is_an_unknown_outcome() {
        let permits = permits(1);
        let control = RequestControl::new(None);
        let result: Result<(), _> = run_blocking(&permits, &control, move || {
            crate::code_generator::current_request_control()
                .unwrap()
                .begin_commit()
                .unwrap();
            panic!("apply failed");
        })
        .await;
        assert_eq!(result.unwrap_err().code, Some(ErrorCode::OutcomeUnknown));
    }

    #[tokio::test]
    async fn a_result_finished_before_a_cancel_is_kept() {
        let permits = permits(1);
        let control = RequestControl::new(None);
        let result = run_blocking(&permits, &control, || Ok(7)).await;
        assert_eq!(result.unwrap(), 7);
        assert_eq!(control.cancel(), crate::execution::Halt::TooLate);
    }
}

//! Running a request's computation under its deadline and cancellation.
//!
//! One [`RequestControl`] spans the whole request: waiting for a compute
//! permit on its lane (see [`super::admission`]), waiting on the blocking
//! pool, and computing. A request stopped
//! while it waits never starts; one stopped while it computes is answered at
//! once and its computation exits at its next cooperative check, which the
//! evaluator makes every few thousand rows it forms, even inside one
//! dataflow step. Only then does it release its permit, so the permits
//! always count the computations actually running. A request that began
//! committing is not interrupted: the wait continues until it finishes, and
//! its result is what it committed.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::admission::{Admission, Lane, Permit, Refusal};
use super::ProgramError;
use crate::execution::{memory, RequestControl, Stop};
use crate::protocol::wire::ErrorCode;
use crate::statement::meta::MetaCommand;
use crate::statement::Statement;

/// How long a stopped computation may take to notice the stop before that
/// is reported as a fault.
const SLOW_EXIT: Duration = Duration::from_secs(1);

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

/// The code of a computation that failed with `code`, unless the request's
/// computation went over a memory limit: then it failed for that, with
/// `resource_exhausted`, even after the request began committing.
pub(crate) fn computation_failure_code(code: ErrorCode) -> ErrorCode {
    let over_memory = crate::code_generator::current_request_control()
        .is_some_and(|c| c.memory_exceeded().is_some());
    if over_memory {
        ErrorCode::ResourceExhausted
    } else {
        code
    }
}

/// Run `job` on the blocking pool under `control`, holding a compute
/// permit of `admission` on `lane` while it computes. `job` sees `control`
/// as the thread's request control, so the evaluator's cooperative checks
/// and the commit boundary observe its deadline and cancellation, and the
/// replication events it appends count as the request's.
pub(super) async fn run_blocking<T, F>(
    admission: &Admission,
    lane: Lane,
    control: &Arc<RequestControl>,
    job: F,
) -> Result<T, ProgramError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, ProgramError> + Send + 'static,
{
    let permit = admit(admission, lane, control).await?;
    let job_control = Arc::clone(control);
    // The request's replication events are appended on the pool's thread.
    let writes = crate::replication::writes::current();
    let task = tokio::task::spawn_blocking(move || {
        // Stopped while queued on the blocking pool: never start.
        if let Some(stop) = job_control.stopped() {
            drop(permit);
            return Err(stop_error(stop));
        }
        let _writes = writes.map(crate::replication::writes::Writes::enter);
        let scope = ControlScope::enter(Arc::clone(&job_control));
        let result = job();
        drop(scope);
        // A computation that held a lot hands what it freed back to the
        // system before its permit admits the next one.
        if job_control.memory_peak() > memory::RELEASE_AFTER_PEAK_BYTES {
            memory::release_freed_memory();
        }
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
/// the commit, so once it starts it is not interrupted. An index build is the
/// exception: it runs under the request's deadline and enters the commit
/// only to install the index it built.
pub(super) fn statement_gate(statement: &Statement) -> Result<(), Stop> {
    let Some(control) = crate::code_generator::current_request_control() else {
        return Ok(());
    };
    match statement {
        Statement::Meta(MetaCommand::IndexCreate(_) | MetaCommand::IndexRebuild(_)) => {
            control.stopped().map_or(Ok(()), Err)
        }
        Statement::Meta(command) if command.changes_durable_state() => control.begin_commit(),
        _ => control.stopped().map_or(Ok(()), Err),
    }
}

/// Wait for a compute permit on `lane` until `control` stops the request or
/// the lane refuses it; see [`super::admission`].
async fn admit(
    admission: &Admission,
    lane: Lane,
    control: &RequestControl,
) -> Result<Permit, ProgramError> {
    let started = Instant::now();
    let permit = admission.acquire(lane, control).await.map_err(|refusal| {
        let queued_ms = started.elapsed().as_millis() as u64;
        match refusal {
            Refusal::Stopped(stop) => {
                // Nothing has started, so nothing can be committing.
                info!(queued_ms, ?stop, lane = lane.name(), "query_stopped_queued");
                stop_error(stop)
            }
            Refusal::QueueFull { lane, limit } => {
                warn!(lane = lane.name(), limit, "query_refused_queue_full");
                overloaded(format!(
                    "Server overloaded: {limit} requests already wait on the {} lane; \
                         nothing ran. Retry later",
                    lane.name()
                ))
            }
            Refusal::Timeout { lane, waited } => {
                warn!(
                    lane = lane.name(),
                    waited_ms = waited.as_millis() as u64,
                    "query_refused_admission_timeout"
                );
                overloaded(format!(
                    "Server overloaded: no compute permit for the {} lane within {} s \
                         (storage.performance.admission.max_wait_ms); nothing ran. Retry later",
                    lane.name(),
                    admission.max_wait().as_secs()
                ))
            }
        }
    })?;
    let queued_ms = started.elapsed().as_millis() as u64;
    if queued_ms > 0 {
        info!(queued_ms, lane = lane.name(), "query_semaphore_wait");
    }
    Ok(permit)
}

/// The error of a request the server could not admit.
fn overloaded(message: String) -> ProgramError {
    ProgramError {
        message,
        code: Some(ErrorCode::Overloaded),
    }
}

/// Await `task` under `control`; see the module docs.
async fn supervise<T: Send + 'static>(
    control: &RequestControl,
    mut task: JoinHandle<Result<T, ProgramError>>,
) -> Result<T, ProgramError> {
    let joined = tokio::select! {
        joined = &mut task => joined,
        stop = control.interrupted() => match stop {
            Some(stop) => {
                warn!(?stop, "query_stopped_running");
                tokio::spawn(report_exit(task, stop));
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

/// Await the computation of a request answered as stopped, and log when it
/// exited, or that it is still running past [`SLOW_EXIT`]: until it exits it
/// holds its thread, its compute permit and its memory.
async fn report_exit<T>(mut task: JoinHandle<T>, stop: Stop) {
    let stopped = Instant::now();
    if tokio::time::timeout(SLOW_EXIT, &mut task).await.is_err() {
        warn!(
            ?stop,
            waited_ms = stopped.elapsed().as_millis() as u64,
            "query_stopped_still_running"
        );
        let _ = task.await;
    }
    info!(
        ?stop,
        exit_ms = stopped.elapsed().as_millis() as u64,
        "query_stopped_exited"
    );
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn permits(n: usize) -> Admission {
        Admission::with_capacity(&crate::config::AdmissionConfig::default(), n)
    }

    /// A job running on `permits`' interactive lane under `control`.
    async fn run<T, F>(
        permits: &Admission,
        control: &Arc<RequestControl>,
        job: F,
    ) -> Result<T, ProgramError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, ProgramError> + Send + 'static,
    {
        run_blocking(permits, Lane::Interactive, control, job).await
    }

    /// Hold every permit of `permits`.
    fn hold_all(permits: &Admission) -> Vec<Permit> {
        permits.try_hold_all().expect("pool idle")
    }

    /// Wait up to five seconds for a permit to be free.
    async fn free_permit(permits: &Admission) -> Permit {
        tokio::time::timeout(
            Duration::from_secs(5),
            permits.acquire(Lane::Interactive, &RequestControl::new(None)),
        )
        .await
        .expect("a permit frees")
        .expect("admitted")
    }

    #[tokio::test]
    async fn a_request_stopped_while_queued_never_runs() {
        let permits = permits(1);
        let held = hold_all(&permits);
        let control = RequestControl::with_timeout(Some(Duration::from_millis(30)));
        let ran = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&ran);
        let result = run(&permits, &control, move || {
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
        let result = run(&permits, &control, move || {
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
        let _permit = free_permit(&permits).await;
        assert!(exited.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_stopped_computation_holds_its_permit_until_it_exits() {
        let permits = permits(1);
        let control = RequestControl::with_timeout(Some(Duration::from_millis(20)));
        let (release, gate) = std::sync::mpsc::channel::<()>();
        let result = run(&permits, &control, move || {
            // A computation between two of its checks.
            gate.recv().ok();
            Ok(())
        })
        .await;
        assert_eq!(result.unwrap_err().code, Some(ErrorCode::DeadlineExceeded));
        // Answered, but still computing: the permit is not free yet.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(permits.in_use(), 1);
        release.send(()).unwrap();
        let _permit = free_permit(&permits).await;
    }

    #[tokio::test]
    async fn a_committing_request_is_awaited_and_reports_its_result() {
        let permits = permits(1);
        let control = RequestControl::with_timeout(Some(Duration::from_millis(20)));
        let result = run(&permits, &control, move || {
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
        let result: Result<(), _> = run(&permits, &control, move || {
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
        let result = run(&permits, &control, || Ok(7)).await;
        assert_eq!(result.unwrap(), 7);
        assert_eq!(control.cancel(), crate::execution::Halt::TooLate);
    }
}

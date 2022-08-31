use std::future::Future;

use crate::handles::SqlResult;

/// Governs the behaviour of of polling in async functions.
///
/// There is a generic implementation for any function retuning a future. This allows e.g. to pass
/// `|| tokio::time::sleep(Duration::from_millis(50))` to functions expecting sleep. That is if
/// you use `tokio` as your async runtime, of course.
pub trait Sleep {
    type Poll: Future;

    /// Between each poll next poll is executed, and the resulting future is awaited.
    fn next_poll(&mut self) -> Self::Poll;
}

impl<S, F> Sleep for S
where
    S: FnMut() -> F,
    F: Future,
{
    type Poll = F;

    fn next_poll(&mut self) -> Self::Poll {
        (self)()
    }
}

pub async fn wait_for<F, O>(mut f: F, sleep: &mut impl Sleep) -> SqlResult<O>
where
    F: FnMut() -> SqlResult<O>,
{
    let mut ret = (f)();
    // Wait for operation to finish, using polling method
    while matches!(ret, SqlResult::StillExecuting) {
        sleep.next_poll().await;
        ret = (f)();
    }
    ret
}

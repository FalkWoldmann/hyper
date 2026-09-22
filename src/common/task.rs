use std::task::{Context, Poll};
#[cfg(feature = "client")]
use std::task::{RawWaker, RawWakerVTable, Waker};

/// A function to help "yield" a future, such that it is re-scheduled immediately.
///
/// Useful for spin counts, so a future doesn't hog too much time.
pub(crate) fn yield_now(cx: &mut Context<'_>) -> Poll<std::convert::Infallible> {
    cx.waker().wake_by_ref();
    Poll::Pending
}

// TODO: replace with `std::task::Waker::noop()` once MSRV >= 1.85
#[cfg(feature = "client")]
pub(crate) fn noop_waker() -> Waker {
    const NOOP_RAW_WAKER: RawWaker = RawWaker::new(std::ptr::null(), &NOOP_VTABLE);
    const NOOP_VTABLE: RawWakerVTable = RawWakerVTable::new(
        // `clone` returns the same noop waker again
        |_: *const ()| NOOP_RAW_WAKER,
        // `wake`, `wake_by_ref`, and `drop` do nothing
        |_: *const ()| {},
        |_: *const ()| {},
        |_: *const ()| {},
    );

    // SAFETY: all functions in the vtable are safe to call, and Waker's safety does not require
    // them to actually do anything.
    unsafe { Waker::from_raw(NOOP_RAW_WAKER) }
}

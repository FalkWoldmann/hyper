//! A oneshot channel, carrying a single value from a `Sender` to a `Receiver`.
//!
//! The sender can also watch for the receiver being dropped, which the client
//! dispatcher uses to notice a caller that has given up on its response.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;

use crate::common::lock::LockResultExt;

#[cfg(all(
    any(feature = "client", feature = "server"),
    any(feature = "http1", feature = "http2"),
))]
pub(crate) fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            value: None,
            sender_open: true,
            receiver_open: true,
        }),
        sender_waker: AtomicWaker::new(),
        receiver_waker: AtomicWaker::new(),
    });

    (
        Sender {
            shared: Arc::clone(&shared),
            sent: false,
        },
        Receiver { shared },
    )
}

#[cfg(all(
    any(feature = "client", feature = "server"),
    any(feature = "http1", feature = "http2"),
))]
pub(crate) struct Sender<T> {
    shared: Arc<Shared<T>>,
    /// The value was handed over, so there is nothing left for `Drop` to do.
    sent: bool,
}

#[must_use = "futures do nothing unless polled"]
pub(crate) struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

/// The `Sender` was dropped without sending a value.
#[derive(Debug)]
pub(crate) struct Canceled;

struct Shared<T> {
    state: Mutex<State<T>>,
    sender_waker: AtomicWaker,
    receiver_waker: AtomicWaker,
}

struct State<T> {
    value: Option<T>,
    sender_open: bool,
    receiver_open: bool,
}

#[cfg(all(
    any(feature = "client", feature = "server"),
    any(feature = "http1", feature = "http2"),
))]
impl<T> Sender<T> {
    /// Send the value, or give it back if the receiver is gone.
    pub(crate) fn send(mut self, value: T) -> Result<(), T> {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        if !state.receiver_open {
            return Err(value);
        }
        state.value = Some(value);
        state.sender_open = false;
        drop(state);
        self.sent = true;
        self.shared.receiver_waker.wake();
        Ok(())
    }

    /// Whether the receiver has been dropped.
    #[cfg(feature = "client")]
    pub(crate) fn is_canceled(&self) -> bool {
        !self.shared.state.lock().panic_if_poisoned().receiver_open
    }

    /// Resolves once the receiver has been dropped.
    #[cfg(feature = "client")]
    pub(crate) fn poll_canceled(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.shared.sender_waker.register(cx.waker());
        if self.is_canceled() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, Canceled>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.shared.receiver_waker.register(cx.waker());
        let mut state = self.shared.state.lock().panic_if_poisoned();
        if let Some(value) = state.value.take() {
            Poll::Ready(Ok(value))
        } else if state.sender_open {
            Poll::Pending
        } else {
            Poll::Ready(Err(Canceled))
        }
    }
}

#[cfg(all(
    any(feature = "client", feature = "server"),
    any(feature = "http1", feature = "http2"),
))]
impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.sent {
            return;
        }
        self.shared.state.lock().panic_if_poisoned().sender_open = false;
        self.shared.receiver_waker.wake();
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        state.receiver_open = false;
        // Drop an unreceived value outside the lock.
        let value = state.value.take();
        drop(state);
        self.shared.sender_waker.wake();
        drop(value);
    }
}

#[cfg(all(
    any(feature = "client", feature = "server"),
    any(feature = "http1", feature = "http2"),
))]
impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

#[cfg(all(
    test,
    any(feature = "client", feature = "server"),
    any(feature = "http1", feature = "http2"),
))]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    use super::channel;

    struct CountWakes(AtomicUsize);

    impl Wake for CountWakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn waker() -> (Arc<CountWakes>, Waker) {
        let counter = Arc::new(CountWakes(AtomicUsize::new(0)));
        (counter.clone(), Waker::from(counter))
    }

    #[test]
    fn send_then_recv() {
        let (tx, mut rx) = channel();
        let (_, w) = waker();
        tx.send(5).unwrap();
        let res = Pin::new(&mut rx).poll(&mut Context::from_waker(&w));
        assert!(matches!(res, Poll::Ready(Ok(5))));
    }

    #[test]
    fn send_wakes_pending_receiver() {
        let (tx, mut rx) = channel();
        let (wakes, w) = waker();
        let cx = &mut Context::from_waker(&w);
        assert!(Pin::new(&mut rx).poll(cx).is_pending());
        tx.send("hi").unwrap();
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        assert!(matches!(Pin::new(&mut rx).poll(cx), Poll::Ready(Ok("hi"))));
    }

    #[test]
    fn dropped_sender_cancels() {
        let (tx, mut rx) = channel::<()>();
        let (wakes, w) = waker();
        let cx = &mut Context::from_waker(&w);
        assert!(Pin::new(&mut rx).poll(cx).is_pending());
        drop(tx);
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        assert!(matches!(Pin::new(&mut rx).poll(cx), Poll::Ready(Err(_))));
    }

    #[cfg(feature = "client")]
    #[test]
    fn dropped_receiver_returns_value_and_wakes_sender() {
        let (mut tx, rx) = channel();
        let (wakes, w) = waker();
        let cx = &mut Context::from_waker(&w);
        assert!(!tx.is_canceled());
        assert!(tx.poll_canceled(cx).is_pending());
        drop(rx);
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        assert!(tx.is_canceled());
        assert!(tx.poll_canceled(cx).is_ready());
        assert_eq!(tx.send(7), Err(7));
    }

    #[test]
    fn unreceived_value_is_dropped_with_receiver() {
        let value = Arc::new(());
        let (tx, rx) = channel();
        tx.send(value.clone()).unwrap();
        assert_eq!(Arc::strong_count(&value), 2);
        drop(rx);
        assert_eq!(Arc::strong_count(&value), 1);
    }

    #[test]
    fn send_across_threads() {
        for _ in 0..100 {
            let (tx, rx) = channel();
            let handle = std::thread::spawn(move || tx.send(1u32).unwrap());
            let (_, w) = waker();
            let mut rx = rx;
            let res = loop {
                match Pin::new(&mut rx).poll(&mut Context::from_waker(&w)) {
                    Poll::Ready(res) => break res,
                    Poll::Pending => std::thread::yield_now(),
                }
            };
            assert_eq!(res.unwrap(), 1);
            handle.join().unwrap();
        }
    }
}

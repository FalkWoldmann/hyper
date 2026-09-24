//! A oneshot channel, carrying a single value from a `Sender` to a `Receiver`.
//!
//! The sender can also watch for the receiver being dropped, which the client
//! dispatcher uses to notice a caller that has given up on its response.
//!
//! An atomic state word records which side finished first. The value and the
//! receiver's waker share one `Mutex`, so storing the value and taking the
//! waker to wake is a single lock for the sender, and checking for the value
//! and registering the waker is a single lock for the receiver. The lock is
//! only contended when the two sides race, and then only briefly.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use atomic_waker::AtomicWaker;

/// The value was stored, and the sender is done.
const VALUE: u8 = 0b001;
/// The sender was dropped without sending.
const SENDER_GONE: u8 = 0b010;
/// The receiver was dropped before completing.
const RECEIVER_GONE: u8 = 0b100;

#[cfg(all(
    any(feature = "client", feature = "server"),
    any(feature = "http1", feature = "http2"),
))]
pub(crate) fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: AtomicU8::new(0),
        slot: Mutex::new(Slot {
            value: None,
            receiver_waker: None,
        }),
        sender_waker: AtomicWaker::new(),
    });

    (
        Sender {
            shared: Arc::clone(&shared),
            sent: false,
        },
        Receiver {
            shared,
            done: false,
        },
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
    /// Completed, so the sender has already sent or left.
    done: bool,
}

/// The `Sender` was dropped without sending a value.
#[derive(Debug)]
pub(crate) struct Canceled;

struct Shared<T> {
    state: AtomicU8,
    slot: Mutex<Slot<T>>,
    sender_waker: AtomicWaker,
}

struct Slot<T> {
    value: Option<T>,
    receiver_waker: Option<Waker>,
}

impl<T> Shared<T> {
    fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    fn slot(&self) -> MutexGuard<'_, Slot<T>> {
        // Only a waker's `clone` or `drop` could panic under the lock, and
        // neither leaves the slot half-updated, so a poisoned slot is still fine.
        self.slot.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(all(
    any(feature = "client", feature = "server"),
    any(feature = "http1", feature = "http2"),
))]
impl<T> Sender<T> {
    /// Send the value, or give it back if the receiver is gone.
    pub(crate) fn send(mut self, value: T) -> Result<(), T> {
        if self.shared.state() & RECEIVER_GONE != 0 {
            return Err(value);
        }
        let waker = {
            let mut slot = self.shared.slot();
            slot.value = Some(value);
            slot.receiver_waker.take()
        };
        self.sent = true;
        let prev = self.shared.state.fetch_or(VALUE, Ordering::AcqRel);
        if prev & RECEIVER_GONE != 0 {
            // The receiver left in between and will never look at the value.
            let value = self.shared.slot().value.take();
            return Err(value.expect("value was just stored"));
        }
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    /// Whether the receiver has been dropped.
    #[cfg(feature = "client")]
    pub(crate) fn is_canceled(&self) -> bool {
        self.shared.state() & RECEIVER_GONE != 0
    }

    /// Resolves once the receiver has been dropped.
    #[cfg(feature = "client")]
    pub(crate) fn poll_canceled(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.is_canceled() {
            return Poll::Ready(());
        }
        self.shared.sender_waker.register(cx.waker());
        // Check again: the receiver may have left before the waker was registered.
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
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(Err(Canceled));
        }
        let mut slot = this.shared.slot();
        // The sender stores the value before setting `VALUE`, so it may be
        // taken here before that flag is set. That's fine: once `done`, this
        // receiver never sets `RECEIVER_GONE`, so the sender keeps the send.
        let res = if let Some(value) = slot.value.take() {
            Ok(value)
        } else if this.shared.state() & SENDER_GONE != 0 {
            // The sender sets `SENDER_GONE` before locking the slot to take
            // the waker, so under the lock it's either seen here or the waker
            // stored below gets woken.
            Err(Canceled)
        } else {
            match &mut slot.receiver_waker {
                Some(waker) => waker.clone_from(cx.waker()),
                empty @ None => *empty = Some(cx.waker().clone()),
            }
            return Poll::Pending;
        };
        drop(slot);
        this.done = true;
        Poll::Ready(res)
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
        let prev = self.shared.state.fetch_or(SENDER_GONE, Ordering::AcqRel);
        if prev & RECEIVER_GONE == 0 {
            let waker = self.shared.slot().receiver_waker.take();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if self.done {
            // The sender already sent or left: nobody is watching.
            return;
        }
        let prev = self.shared.state.fetch_or(RECEIVER_GONE, Ordering::AcqRel);
        if prev & VALUE != 0 {
            // Drop the unreceived value outside the lock.
            let value = self.shared.slot().value.take();
            drop(value);
        } else if prev & SENDER_GONE == 0 {
            self.shared.sender_waker.wake();
        }
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
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    use super::channel;

    #[derive(Default)]
    struct CountWakes(AtomicUsize);

    impl Wake for CountWakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl CountWakes {
        fn waker() -> (Arc<Self>, Waker) {
            let counter = Arc::new(Self::default());
            (Arc::clone(&counter), Waker::from(counter))
        }

        fn count(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn send_then_recv() {
        let (tx, rx) = channel();
        tx.send(5).unwrap();
        let res = pin!(rx).poll(&mut Context::from_waker(Waker::noop()));
        assert!(matches!(res, Poll::Ready(Ok(5))));
    }

    #[test]
    fn send_wakes_pending_receiver() {
        let (tx, rx) = channel();
        let mut rx = pin!(rx);
        let (wakes, waker) = CountWakes::waker();
        let cx = &mut Context::from_waker(&waker);
        assert!(rx.as_mut().poll(cx).is_pending());
        tx.send("hi").unwrap();
        assert_eq!(wakes.count(), 1);
        assert!(matches!(rx.poll(cx), Poll::Ready(Ok("hi"))));
    }

    #[test]
    fn send_wakes_latest_waker() {
        let (tx, rx) = channel();
        let mut rx = pin!(rx);
        let (old_wakes, old_waker) = CountWakes::waker();
        let (new_wakes, new_waker) = CountWakes::waker();
        assert!(rx
            .as_mut()
            .poll(&mut Context::from_waker(&old_waker))
            .is_pending());
        assert!(rx
            .as_mut()
            .poll(&mut Context::from_waker(&new_waker))
            .is_pending());
        tx.send(()).unwrap();
        assert_eq!(old_wakes.count(), 0);
        assert_eq!(new_wakes.count(), 1);
    }

    #[test]
    fn dropped_sender_cancels() {
        let (tx, rx) = channel::<()>();
        let mut rx = pin!(rx);
        let (wakes, waker) = CountWakes::waker();
        let cx = &mut Context::from_waker(&waker);
        assert!(rx.as_mut().poll(cx).is_pending());
        drop(tx);
        assert_eq!(wakes.count(), 1);
        assert!(matches!(rx.poll(cx), Poll::Ready(Err(_))));
    }

    #[cfg(feature = "client")]
    #[test]
    fn dropped_receiver_returns_value_and_wakes_sender() {
        let (mut tx, rx) = channel();
        let (wakes, waker) = CountWakes::waker();
        let cx = &mut Context::from_waker(&waker);
        assert!(!tx.is_canceled());
        assert!(tx.poll_canceled(cx).is_pending());
        drop(rx);
        assert_eq!(wakes.count(), 1);
        assert!(tx.is_canceled());
        assert!(tx.poll_canceled(cx).is_ready());
        assert_eq!(tx.send(7), Err(7));
    }

    #[test]
    fn unreceived_value_is_dropped_with_receiver() {
        let value = Arc::new(());
        let (tx, rx) = channel();
        tx.send(Arc::clone(&value)).unwrap();
        assert_eq!(Arc::strong_count(&value), 2);
        drop(rx);
        assert_eq!(Arc::strong_count(&value), 1);
    }

    #[test]
    fn received_value_is_not_dropped_twice() {
        let value = Arc::new(());
        let (tx, rx) = channel();
        tx.send(Arc::clone(&value)).unwrap();
        let mut rx = pin!(rx);
        let Poll::Ready(Ok(got)) = rx.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
            panic!("value should be ready");
        };
        drop(rx);
        assert_eq!(Arc::strong_count(&value), 2);
        drop(got);
        assert_eq!(Arc::strong_count(&value), 1);
    }

    #[test]
    fn send_races_receiver_drop() {
        // Whichever side goes second must see the other: the value is either
        // dropped by the receiver or handed back to the sender, exactly once.
        for _ in 0..200 {
            let value = Arc::new(());
            let (tx, rx) = channel();
            let sent = Arc::clone(&value);
            let sender = std::thread::spawn(move || tx.send(sent).is_ok());
            drop(rx);
            let _ = sender.join().unwrap();
            assert_eq!(Arc::strong_count(&value), 1);
        }
    }

    #[test]
    fn send_across_threads() {
        struct Unpark(std::thread::Thread);

        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }

        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let cx = &mut Context::from_waker(&waker);
        for _ in 0..100 {
            let (tx, rx) = channel();
            let sender = std::thread::spawn(move || tx.send(1u32).unwrap());
            let mut rx = pin!(rx);
            let res = loop {
                match rx.as_mut().poll(cx) {
                    Poll::Ready(res) => break res,
                    // A lost wakeup would hang here.
                    Poll::Pending => std::thread::park(),
                }
            };
            assert_eq!(res.unwrap(), 1);
            sender.join().unwrap();
        }
    }
}

//! An unbounded multi-producer, single-consumer queue for the client dispatcher.
//!
//! Built on `std::sync::mpsc` (a lock-free linked list of blocks since Rust
//! 1.67, whose `Sender` is `Sync` since 1.72), plus an `AtomicWaker` so the
//! receiver can be polled from async code. The dispatcher bounds it itself,
//! with `want`.

#[cfg(feature = "http1")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;

pub(crate) fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = mpsc::channel();
    let shared = Arc::new(Shared {
        receiver_waker: AtomicWaker::new(),
        #[cfg(feature = "http1")]
        closed: AtomicBool::new(false),
    });

    (
        Sender {
            tx,
            notify: Notify(Arc::clone(&shared)),
        },
        Receiver {
            rx: Mutex::new(rx),
            shared,
        },
    )
}

struct Shared {
    receiver_waker: AtomicWaker,
    /// Set by `Receiver::close`: new messages are refused, queued ones stay.
    #[cfg(feature = "http1")]
    closed: AtomicBool,
}

pub(crate) struct Sender<T> {
    tx: mpsc::Sender<T>,
    // Declared after `tx`, so it's dropped after it: the receiver it wakes can
    // then observe the disconnect.
    notify: Notify,
}

/// Wakes the receiver when a sender is dropped.
struct Notify(Arc<Shared>);

pub(crate) struct Receiver<T> {
    /// Never locked, only reached through `get_mut`: the `Mutex` just keeps
    /// the receiver `Sync` (`mpsc::Receiver` is not), as the public connection
    /// types that hold it are.
    rx: Mutex<mpsc::Receiver<T>>,
    shared: Arc<Shared>,
}

impl<T> Sender<T> {
    /// Queue a message, or give it back if the receiver is closed or gone.
    pub(crate) fn send(&self, value: T) -> Result<(), T> {
        let shared = &self.notify.0;
        #[cfg(feature = "http1")]
        if shared.closed.load(Ordering::Acquire) {
            return Err(value);
        }
        self.tx.send(value).map_err(|err| err.0)?;
        shared.receiver_waker.wake();
        Ok(())
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            tx: self.tx.clone(),
            notify: Notify(Arc::clone(&self.notify.0)),
        }
    }
}

impl Drop for Notify {
    fn drop(&mut self) {
        self.0.receiver_waker.wake();
    }
}

impl<T> Receiver<T> {
    fn rx(&mut self) -> &mut mpsc::Receiver<T> {
        // Never locked, so never poisoned.
        self.rx.get_mut().unwrap_or_else(PoisonError::into_inner)
    }

    fn try_poll(&mut self) -> Poll<Option<T>> {
        match self.rx().try_recv() {
            Ok(value) => Poll::Ready(Some(value)),
            Err(mpsc::TryRecvError::Disconnected) => Poll::Ready(None),
            Err(mpsc::TryRecvError::Empty) => Poll::Pending,
        }
    }

    /// The next message, `None` once every sender is gone and the queue is empty.
    pub(crate) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if let ready @ Poll::Ready(_) = self.try_poll() {
            return ready;
        }
        self.shared.receiver_waker.register(cx.waker());
        // Check again: a message may have arrived before the waker was registered.
        self.try_poll()
    }

    /// The next message, if one is queued. Doesn't register for wakeups.
    #[cfg(feature = "http1")]
    pub(crate) fn try_recv(&mut self) -> Option<T> {
        self.rx().try_recv().ok()
    }

    /// Stop accepting messages. Already queued ones can still be received.
    #[cfg(feature = "http1")]
    pub(crate) fn close(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    use super::unbounded;

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
    fn fifo() {
        let (tx, mut rx) = unbounded();
        for i in 0..5 {
            tx.send(i).unwrap();
        }
        let cx = &mut Context::from_waker(Waker::noop());
        for i in 0..5 {
            assert_eq!(rx.poll_recv(cx), Poll::Ready(Some(i)));
        }
        assert_eq!(rx.poll_recv(cx), Poll::Pending);
    }

    #[test]
    fn send_wakes_receiver() {
        let (tx, mut rx) = unbounded();
        let (wakes, waker) = CountWakes::waker();
        let cx = &mut Context::from_waker(&waker);
        assert_eq!(rx.poll_recv(cx), Poll::Pending);
        tx.send(1).unwrap();
        assert_eq!(wakes.count(), 1);
        assert_eq!(rx.poll_recv(cx), Poll::Ready(Some(1)));
    }

    #[test]
    fn ends_after_last_sender_and_drained() {
        let (tx, mut rx) = unbounded();
        let tx2 = tx.clone();
        tx.send(1).unwrap();
        drop(tx);
        let (wakes, waker) = CountWakes::waker();
        let cx = &mut Context::from_waker(&waker);
        assert_eq!(rx.poll_recv(cx), Poll::Ready(Some(1)));
        assert_eq!(rx.poll_recv(cx), Poll::Pending);
        drop(tx2);
        assert_eq!(wakes.count(), 1);
        assert_eq!(rx.poll_recv(cx), Poll::Ready(None));
    }

    #[cfg(feature = "http1")]
    #[test]
    fn close_rejects_new_but_keeps_queued() {
        let (tx, mut rx) = unbounded();
        tx.send(1).unwrap();
        rx.close();
        assert_eq!(tx.send(2), Err(2));
        assert_eq!(rx.try_recv(), Some(1));
        assert_eq!(rx.try_recv(), None);
    }

    #[test]
    fn dropped_receiver_drops_queued_and_rejects() {
        let value = Arc::new(());
        let (tx, rx) = unbounded();
        tx.send(Arc::clone(&value)).unwrap();
        drop(rx);
        assert_eq!(Arc::strong_count(&value), 1);
        assert!(tx.send(Arc::clone(&value)).is_err());
    }

    #[test]
    fn many_threads() {
        // Miri is slow with threads, and still finds races with fewer messages.
        const PER: usize = if cfg!(miri) { 50 } else { 1_000 };

        struct Unpark(std::thread::Thread);

        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }

        let (tx, mut rx) = unbounded();
        let senders: Vec<_> = (0..4)
            .map(|_| {
                let tx = tx.clone();
                std::thread::spawn(move || {
                    for i in 0..PER {
                        tx.send(i).unwrap();
                    }
                })
            })
            .collect();
        drop(tx);

        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let cx = &mut Context::from_waker(&waker);
        let mut received = 0;
        loop {
            match rx.poll_recv(cx) {
                Poll::Ready(Some(_)) => received += 1,
                Poll::Ready(None) => break,
                // A lost wakeup would hang here.
                Poll::Pending => std::thread::park(),
            }
        }
        for sender in senders {
            sender.join().unwrap();
        }
        assert_eq!(received, 4 * PER);
    }
}

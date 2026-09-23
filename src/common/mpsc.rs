//! An unbounded multi-producer, single-consumer queue.
//!
//! The client dispatcher bounds it itself (with `want`), so it only needs to
//! hand requests from senders to the connection task. Messages live in a
//! `VecDeque`, so steady-state sends don't allocate.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;

use crate::common::lock::LockResultExt;

pub(crate) fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            queue: VecDeque::new(),
            senders: 1,
            receiver_open: true,
        }),
        receiver_waker: AtomicWaker::new(),
    });

    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver { shared },
    )
}

pub(crate) struct Sender<T> {
    shared: Arc<Shared<T>>,
}

pub(crate) struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

struct Shared<T> {
    state: Mutex<State<T>>,
    receiver_waker: AtomicWaker,
}

struct State<T> {
    queue: VecDeque<T>,
    senders: usize,
    receiver_open: bool,
}

impl<T> Sender<T> {
    /// Queue a message, or give it back if the receiver is closed.
    pub(crate) fn send(&self, value: T) -> Result<(), T> {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        if !state.receiver_open {
            return Err(value);
        }
        state.queue.push_back(value);
        drop(state);
        self.shared.receiver_waker.wake();
        Ok(())
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.state.lock().panic_if_poisoned().senders += 1;
        Sender {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        state.senders -= 1;
        let last = state.senders == 0;
        drop(state);
        if last {
            self.shared.receiver_waker.wake();
        }
    }
}

impl<T> Receiver<T> {
    /// The next message, `None` once every sender is gone and the queue is empty.
    pub(crate) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.shared.receiver_waker.register(cx.waker());
        let mut state = self.shared.state.lock().panic_if_poisoned();
        if let Some(value) = state.queue.pop_front() {
            Poll::Ready(Some(value))
        } else if state.senders == 0 {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    /// The next message, if one is queued. Doesn't register for wakeups.
    #[cfg(feature = "http1")]
    pub(crate) fn try_recv(&mut self) -> Option<T> {
        self.shared
            .state
            .lock()
            .panic_if_poisoned()
            .queue
            .pop_front()
    }

    /// Stop accepting messages. Already queued ones can still be received.
    #[cfg(feature = "http1")]
    pub(crate) fn close(&mut self) {
        self.shared.state.lock().panic_if_poisoned().receiver_open = false;
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        state.receiver_open = false;
        // Drop queued messages outside the lock: their destructors can run
        // arbitrary code, such as failing a request's callback.
        let queue = std::mem::take(&mut state.queue);
        drop(state);
        drop(queue);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    use super::unbounded;

    struct Noop;

    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }

    fn noop_cx<R>(f: impl FnOnce(&mut Context<'_>) -> R) -> R {
        let waker = Waker::from(Arc::new(Noop));
        f(&mut Context::from_waker(&waker))
    }

    #[test]
    fn fifo() {
        let (tx, mut rx) = unbounded();
        for i in 0..5 {
            tx.send(i).unwrap();
        }
        noop_cx(|cx| {
            for i in 0..5 {
                assert_eq!(rx.poll_recv(cx), Poll::Ready(Some(i)));
            }
            assert_eq!(rx.poll_recv(cx), Poll::Pending);
        });
    }

    #[test]
    fn ends_after_last_sender_and_drained() {
        let (tx, mut rx) = unbounded();
        let tx2 = tx.clone();
        tx.send(1).unwrap();
        drop(tx);
        noop_cx(|cx| {
            assert_eq!(rx.poll_recv(cx), Poll::Ready(Some(1)));
            assert_eq!(rx.poll_recv(cx), Poll::Pending);
            drop(tx2);
            assert_eq!(rx.poll_recv(cx), Poll::Ready(None));
        });
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
        tx.send(value.clone()).unwrap();
        drop(rx);
        assert_eq!(Arc::strong_count(&value), 1);
        assert!(tx.send(value.clone()).is_err());
    }

    #[test]
    fn many_threads() {
        const PER: usize = 1_000;
        let (tx, mut rx) = unbounded();
        let handles: Vec<_> = (0..4)
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

        let thread = std::thread::current();
        struct Unpark(std::thread::Thread);
        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(Unpark(thread)));
        let cx = &mut Context::from_waker(&waker);
        let mut received = 0;
        loop {
            match rx.poll_recv(cx) {
                Poll::Ready(Some(_)) => received += 1,
                Poll::Ready(None) => break,
                Poll::Pending => std::thread::park(),
            }
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(received, 4 * PER);
    }
}

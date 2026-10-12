#![allow(dead_code)]
//! Various runtimes for hyper
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use hyper::rt::{Sleep, Timer};
use pin_project_lite::pin_project;

#[derive(Clone)]
/// An Executor that uses the tokio runtime.
pub struct TokioExecutor;

impl<F> hyper::rt::Executor<F> for TokioExecutor
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::task::spawn(fut);
    }
}

/// A Timer that uses the tokio runtime.

#[derive(Clone, Debug)]
pub struct TokioTimer;

impl Timer for TokioTimer {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Sleep>> {
        Box::pin(TokioSleep {
            inner: tokio::time::sleep(duration),
        })
    }

    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Sleep>> {
        Box::pin(TokioSleep {
            inner: tokio::time::sleep_until(deadline.into()),
        })
    }

    fn now(&self) -> Instant {
        tokio::time::Instant::now().into()
    }

    fn reset(&self, sleep: &mut Pin<Box<dyn Sleep>>, new_deadline: Instant) {
        if let Some(sleep) = sleep.as_mut().downcast_mut_pin::<TokioSleep>() {
            sleep.reset(new_deadline)
        }
    }
}

impl TokioTimer {
    /// Create a new TokioTimer
    pub fn new() -> Self {
        Self {}
    }
}

// Use TokioSleep to get tokio::time::Sleep to implement Unpin.
// see https://docs.rs/tokio/latest/tokio/time/struct.Sleep.html
pin_project! {
    pub(crate) struct TokioSleep {
        #[pin]
        pub(crate) inner: tokio::time::Sleep,
    }
}

impl Future for TokioSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

impl Sleep for TokioSleep {}

impl TokioSleep {
    pub fn reset(self: Pin<&mut Self>, deadline: Instant) {
        self.project().inner.as_mut().reset(deadline.into());
    }
}

pin_project! {
    #[derive(Debug)]
    pub struct TokioIo<T> {
        #[pin]
        inner: T,
        owned: bool,
        lent_read: Option<bytes::BytesMut>,
        lent_write: Vec<bytes::Bytes>,
    }
}

/// With `HYPER_TEST_OWNED_IO` set, `TokioIo` behaves like completion-based
/// I/O: it needs owned buffers, and keeps them from when an operation is
/// submitted until a later poll completes it.
fn owned_io() -> bool {
    static OWNED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *OWNED.get_or_init(|| std::env::var_os("HYPER_TEST_OWNED_IO").is_some())
}

impl<T> TokioIo<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            owned: owned_io(),
            lent_read: None,
            lent_write: Vec::new(),
        }
    }

    pub fn inner(self) -> T {
        self.inner
    }
}

impl<T> hyper::rt::Read for TokioIo<T>
where
    T: tokio::io::AsyncRead,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let n = unsafe {
            let mut tbuf = tokio::io::ReadBuf::uninit(buf.as_mut());
            match tokio::io::AsyncRead::poll_read(self.project().inner, cx, &mut tbuf) {
                Poll::Ready(Ok(())) => tbuf.filled().len(),
                other => return other,
            }
        };

        unsafe {
            buf.advance(n);
        }
        Poll::Ready(Ok(()))
    }

    fn poll_read_buf(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut bytes::BytesMut,
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = self.project();
        let mut spare = match this.lent_read.take() {
            Some(spare) => spare,
            None => {
                if buf.capacity() == buf.len() {
                    buf.reserve(64);
                }
                let spare = buf.split_off(buf.len());
                if *this.owned {
                    // Submitted. Complete on a later poll.
                    *this.lent_read = Some(spare);
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                spare
            }
        };
        let res = {
            let mut tbuf = tokio::io::ReadBuf::uninit(spare.spare_capacity_mut());
            match tokio::io::AsyncRead::poll_read(this.inner, cx, &mut tbuf) {
                Poll::Ready(res) => res.map(|()| tbuf.filled().len()),
                Poll::Pending if *this.owned => {
                    *this.lent_read = Some(spare);
                    return Poll::Pending;
                }
                Poll::Pending => {
                    buf.unsplit(spare);
                    return Poll::Pending;
                }
            }
        };
        let n = match res {
            Ok(n) => n,
            Err(e) => {
                buf.unsplit(spare);
                return Poll::Ready(Err(e));
            }
        };
        unsafe {
            spare.set_len(n);
        }
        // The read was submitted for another buffer if the IO changed hands
        // in between. `buf` only gets what fits its spare capacity then.
        assert!(
            buf.as_ptr().wrapping_add(buf.len()) == spare.as_ptr()
                || n <= buf.capacity() - buf.len(),
            "a read completed with more than the spare capacity of the buffer"
        );
        buf.unsplit(spare);
        Poll::Ready(Ok(n))
    }
}

impl<T> hyper::rt::Write for TokioIo<T>
where
    T: tokio::io::AsyncWrite,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        tokio::io::AsyncWrite::poll_write(self.project().inner, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        tokio::io::AsyncWrite::poll_flush(self.project().inner, cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        tokio::io::AsyncWrite::poll_shutdown(self.project().inner, cx)
    }

    fn is_write_vectored(&self) -> bool {
        tokio::io::AsyncWrite::is_write_vectored(&self.inner)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        tokio::io::AsyncWrite::poll_write_vectored(self.project().inner, cx, bufs)
    }

    fn needs_owned_buffers(&self) -> bool {
        self.owned
    }

    fn poll_write_owned(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[bytes::Bytes],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = self.project();
        if this.lent_write.is_empty() {
            this.lent_write
                .extend(bufs.iter().filter(|buf| !buf.is_empty()).cloned());
            if this.lent_write.is_empty() {
                return Poll::Ready(Ok(0));
            }
            if *this.owned {
                // Submitted. Complete on a later poll.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        } else {
            let same = this
                .lent_write
                .iter()
                .zip(bufs.iter().filter(|buf| !buf.is_empty()))
                .all(|(a, b)| a.as_ptr() == b.as_ptr() && a.len() == b.len());
            assert!(same, "other buffers were passed to a write in flight");
        }
        let slices: Vec<std::io::IoSlice<'_>> = this
            .lent_write
            .iter()
            .map(|buf| std::io::IoSlice::new(buf))
            .collect();
        let res = if tokio::io::AsyncWrite::is_write_vectored(&*this.inner) {
            tokio::io::AsyncWrite::poll_write_vectored(this.inner, cx, &slices)
        } else {
            tokio::io::AsyncWrite::poll_write(this.inner, cx, &slices[0])
        };
        if res.is_ready() || !*this.owned {
            drop(slices);
            this.lent_write.clear();
        }
        res
    }
}

impl<T> tokio::io::AsyncRead for TokioIo<T>
where
    T: hyper::rt::Read,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        tbuf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        //let init = tbuf.initialized().len();
        let filled = tbuf.filled().len();
        let sub_filled = unsafe {
            let mut buf = hyper::rt::ReadBuf::uninit(tbuf.unfilled_mut());

            match hyper::rt::Read::poll_read(self.project().inner, cx, buf.unfilled()) {
                Poll::Ready(Ok(())) => buf.filled().len(),
                other => return other,
            }
        };

        let n_filled = filled + sub_filled;
        // At least sub_filled bytes had to have been initialized.
        let n_init = sub_filled;
        unsafe {
            tbuf.assume_init(n_init);
            tbuf.set_filled(n_filled);
        }

        Poll::Ready(Ok(()))
    }
}

impl<T> tokio::io::AsyncWrite for TokioIo<T>
where
    T: hyper::rt::Write,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        hyper::rt::Write::poll_write(self.project().inner, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        hyper::rt::Write::poll_flush(self.project().inner, cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        hyper::rt::Write::poll_shutdown(self.project().inner, cx)
    }

    fn is_write_vectored(&self) -> bool {
        hyper::rt::Write::is_write_vectored(&self.inner)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        hyper::rt::Write::poll_write_vectored(self.project().inner, cx, bufs)
    }
}

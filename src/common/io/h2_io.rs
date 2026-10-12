use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};

/// This adapts from `hyper` IO traits to the ones in `h2`.
#[derive(Debug)]
pub(crate) struct H2Io<T>(pub(crate) T);

impl<T> H2Io<T> {
    pub(crate) fn new(io: T) -> Self {
        H2Io(io)
    }

    fn p(self: Pin<&mut Self>) -> Pin<&mut T> {
        // SAFETY: The simplest of projections. This is just
        // a wrapper, we don't do anything that would undo the projection.
        unsafe { self.map_unchecked_mut(|me| &mut me.0) }
    }
}

impl<T> h2::io::Read for H2Io<T>
where
    T: crate::rt::Read,
{
    fn poll_read_buf(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut BytesMut,
    ) -> Poll<io::Result<usize>> {
        crate::rt::Read::poll_read_buf(self.p(), cx, buf)
    }
}

impl<T> h2::io::Write for H2Io<T>
where
    T: crate::rt::Write,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        crate::rt::Write::poll_write(self.p(), cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        crate::rt::Write::poll_write_vectored(self.p(), cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        crate::rt::Write::is_write_vectored(&self.0)
    }

    fn needs_owned_buffers(&self) -> bool {
        crate::rt::Write::needs_owned_buffers(&self.0)
    }

    fn poll_write_owned(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[Bytes],
    ) -> Poll<io::Result<usize>> {
        crate::rt::Write::poll_write_owned(self.p(), cx, bufs)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        crate::rt::Write::poll_flush(self.p(), cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        crate::rt::Write::poll_shutdown(self.p(), cx)
    }
}

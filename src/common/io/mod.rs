#[cfg(all(test, any(feature = "client", feature = "server"), feature = "http2"))]
mod compat;
#[cfg(all(any(feature = "client", feature = "server"), feature = "http2"))]
mod h2_io;
mod rewind;

#[cfg(all(test, any(feature = "client", feature = "server"), feature = "http2"))]
pub(crate) use self::compat::Compat;
#[cfg(all(any(feature = "client", feature = "server"), feature = "http2"))]
pub(crate) use self::h2_io::H2Io;
pub(crate) use self::rewind::Rewind;

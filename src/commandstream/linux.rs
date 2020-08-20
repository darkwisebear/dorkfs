use std::{
    io,
    path::{Path, PathBuf},
    task::{Context, Poll},
    pin::Pin,
};

use tokio::{
    net::{UnixListener, UnixStream},
    stream::Stream,
    time::{timeout, Elapsed}
};

use tempfile::TempDir;
use log::debug;

pub(super) struct UnixPipe {
    incoming: UnixListener,
    _tempdir: TempDir,
    path: PathBuf
}

impl UnixPipe {
    fn new() -> io::Result<Self> {
        let tempdir = TempDir::new()?;
        let path = tempdir.path().join("cmd");
        let incoming = UnixListener::bind(&path)?;
        Ok(Self {
            incoming,
            _tempdir: tempdir,
            path
        })
    }
}

impl Stream for UnixPipe {
    type Item = <UnixListener as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.incoming).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.incoming.size_hint()
    }
}

pub(super) fn create_communication_server() -> io::Result<(UnixPipe, String)> {
    let pipe = UnixPipe::new()?;
    let path = pipe.path.display().to_string();
    debug!("Creating command socket at {}", &path);

    Ok((pipe, path))
}

pub(super) async fn open_communication_channel(dorkdir: impl AsRef<Path>) -> io::Result<UnixStream> {
    let path = dorkdir.as_ref().join("cmd");
    match timeout(super::COMMAND_CHANNEL_CONNECT_TIMEOUT, UnixStream::connect(&path)).await {
        Ok(stream) => stream,
        Err(Elapsed { .. }) => Err(io::ErrorKind::TimedOut.into())
    }
}

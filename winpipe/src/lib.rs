use std::{
    ffi::{OsStr, OsString},
    pin::Pin,
    task::{Context, Poll},
    io,
    mem::replace,
    os::windows::{
        fs::OpenOptionsExt,
        io::{IntoRawHandle, FromRawHandle}
    },
    time::{SystemTime, Duration},
    fs::{File, OpenOptions}
};

use mio_named_pipes as mnp;
use miow;

use tokio::stream::Stream;
use tokio::io::PollEvented;

use log::trace;

use winapi::{
    um::winbase,
    shared::winerror
};

pub type PollNamedPipe = PollEvented<mnp::NamedPipe>;

pub struct NamedPipeServer {
    name: OsString
}

impl NamedPipeServer {
    pub fn bind<A: Into<OsString>>(name: A) -> NamedPipeServer {
        NamedPipeServer { name: name.into() }
    }

    pub fn incoming(self) -> Result<Incoming, io::Error> {
        let pipe = mnp::NamedPipe::new(&self.name)?;
        PollEvented::new(pipe)
            .map(|pipe|
                Incoming {
                    pipe,
                    name: self.name,
                    connection_attempted: false
               })
    }
}

pub struct Incoming {
    pipe: PollNamedPipe,
    name: OsString,
    connection_attempted: bool,
}

impl Incoming {
    fn replace_pipe_instance(&mut self) -> io::Result<PollNamedPipe> {
        self.connection_attempted = false;
        let new_miow_pipe =
            miow::pipe::NamedPipeBuilder::new(&self.name)
                .first(false)
                .create()?;
        let new_pipe = unsafe {
            mnp::NamedPipe::from_raw_handle(new_miow_pipe.into_raw_handle())
        };

        PollEvented::new(new_pipe)
            .map(|poll_evented| replace(&mut self.pipe, poll_evented))
    }
}

impl Stream for Incoming {
    type Item = io::Result<PollNamedPipe>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.pipe.poll_write_ready(cx) {
            Poll::Ready(Ok(_)) => {
                trace!("Server: Received write readiness");
                Poll::Ready(Some(self.replace_pipe_instance()))
            },
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => {
                trace!("Server: Connection pending");
                if !self.connection_attempted {
                    match self.pipe.get_ref().connect() {
                        Err(err) if err.kind() != io::ErrorKind::WouldBlock =>
                            return Poll::Ready(Some(Err(err))),
                        _ => self.connection_attempted = true
                    }
                }
                Poll::Pending
            }
        }
    }
}

pub fn connect_named_pipe<S: AsRef<OsStr>>(name: S, timeout: Option<Duration>) -> io::Result<PollNamedPipe> {
    let start_time = SystemTime::now();
    loop {
        let timeout = timeout
            .map(|timeout| {
                let elapsed = SystemTime::now().duration_since(start_time).unwrap();
                timeout.checked_sub(elapsed)
                    .ok_or(io::Error::from(io::ErrorKind::TimedOut))
            })
            .transpose()?;
        trace!("Client: Waiting for pipe instance...");
        miow::pipe::NamedPipe::wait(name.as_ref(), timeout)?;
        trace!("Client: Opening pipe instance");
        let opened_file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(winbase::FILE_FLAG_OVERLAPPED)
            .open(name.as_ref())
            .map(File::into_raw_handle)
            .map(|handle| unsafe { mnp::NamedPipe::from_raw_handle(handle) });
        match opened_file {
            Ok(file) => {
                trace!("Client: Opened pipe");
                break PollEvented::new(file)
            },
            Err(err) if err.raw_os_error() == Some(winerror::ERROR_PIPE_BUSY as i32) => {
                trace!("Client: Unable to open pipe after wait, retrying...");
                continue
            },
            Err(err) if err.raw_os_error() == Some(winerror::ERROR_SEM_TIMEOUT as i32) => {
                trace!("Client: Timeout waiting for a free pipe slot");
                break Err(io::ErrorKind::TimedOut.into())
            }
            Err(err) => {
                trace!("Client: Error opening pipe: {}", err);
                break Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        iter::FromIterator,
        sync::{
            Arc,
            atomic::{
                Ordering,
                AtomicUsize
            },
        },
    };

    use tokio::{
        self,
        task::spawn_blocking,
        stream::StreamExt,
        io::{self, AsyncWriteExt},
    };

    use log::trace;
    use simple_logger;

    use super::*;

    #[tokio::test]
    async fn it_works() {
        simple_logger::init().unwrap();

        let test_strs = [&b"foo"[..], &b"barz"[..], &b"nsjdljsadjjfljsadsad"[..]];

        let mut incoming = NamedPipeServer::bind("\\\\.\\pipe\\testpipe")
            .incoming()
            .unwrap()
            .take(test_strs.len());

        static WRITER_NUMBER: AtomicUsize = AtomicUsize::new(0);

        let make_writer= |data: &'static [u8]| tokio::spawn(async move {
            let my_num = WRITER_NUMBER.fetch_add(1, Ordering::Relaxed);
            trace!("Writer #{}", my_num);
            let mut pipe = spawn_blocking(||
                connect_named_pipe("\\\\.\\pipe\\testpipe", None)
                    .unwrap()).await.unwrap();
            let len = data.len();
            assert_eq!(len, pipe.write(data).await.unwrap());
            pipe.shutdown().await.unwrap();
            drop(pipe.into_inner().unwrap());
        });

        let clients = test_strs.iter().map(|&s| make_writer(s)).collect::<Vec<_>>();

        let results = Arc::new(HashSet::<&'static [u8]>::from_iter(test_strs.iter().cloned()));

        static SERVER_NUMBER: AtomicUsize = AtomicUsize::new(0);

        let server = tokio::spawn(async move {
            while let Some(mut pipe) = incoming.try_next().await.unwrap() {
                let results = Arc::clone(&results);
                tokio::spawn(async move {
                    let my_num = SERVER_NUMBER.fetch_add(1, Ordering::Relaxed);
                    trace!("Reader #{}", my_num);
                    let mut buf = Vec::with_capacity(256);
                    let num_read = io::copy(&mut pipe, &mut buf).await.unwrap();
                    assert!(results.contains(&buf[0..num_read as usize]));
                });
            }
        });

        server.await.unwrap();
        for client in clients {
            client.await.unwrap();
        }
    }
}

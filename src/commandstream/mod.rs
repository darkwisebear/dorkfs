use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    io,
    net::Shutdown
};

use crate::{
    cache::CacheRef,
    overlay::WorkspaceController
};

use futures::{failed, Future, Stream, IntoFuture, future::Either};
use tokio::{
    self,
    prelude::{AsyncRead, AsyncWrite}
};
use failure;
use serde_json;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Command {
    Commit {
        message: String
    },

    Log {
        #[serde(default)]
        start_commit: Option<CacheRef>
    }
}

pub struct CommandExecutor<W: for<'a> WorkspaceController<'a>+'static> {
    controller: Arc<RwLock<W>>
}

// We need to manually implement this as derive(Clone) will only work
// if W: Clone.
impl<W: for<'a> WorkspaceController<'a>+'static> Clone for CommandExecutor<W> {
    fn clone(&self) -> Self {
        Self {
            controller: Arc::clone(&self.controller)
        }
    }
}

impl<W: for<'a> WorkspaceController<'a>+'static> CommandExecutor<W> {
    pub fn new(controller: Arc<RwLock<W>>) -> Self {
        Self {
            controller
        }
    }

    pub fn command<T: AsyncWrite+Send+Sync+'static>(&self, command: Command, target: T)
        -> impl Future<Item=T, Error=failure::Error> {
        match command {
            Command::Commit { message } =>
                Box::new(self.commit_handler(message, target))
                    as Box<dyn Future<Item=T, Error=failure::Error>+Send>,

            Command::Log { start_commit } =>
                Box::new(self.handle_log_stream(start_commit, target))
                             as Box<dyn Future<Item=T, Error=failure::Error>+Send>
        }
    }

    fn handle_log_stream<T>(&self, start_commit: Option<CacheRef>, target: T)
        -> impl Future<Item=T, Error=failure::Error>+Send
        where T: AsyncWrite+Send+Sync+'static {
        static NO_LOG_STRING: &[u8] = b"(empty log)";

        let controller = self.controller.read().unwrap();

        let start_commit = match start_commit {
            Some(start_commit) => Ok(Some(start_commit)),
            None => controller.get_current_head_ref()
                .map_err(failure::Error::from)
        };

        match start_commit {
            Ok(start_commit) => {
                let ok_future = match start_commit {
                    Some(start_commit) => {
                        let log_stream = controller.get_log_stream(start_commit)
                                            .map_err(failure::Error::from);
                        Either::A(log_stream
                            .fold(target, |target, commit| {
                                let mut commit_string = commit.to_string();
                                commit_string.push('\n');
                                tokio::io::write_all(target, commit_string)
                                    .map(|(target, _)| target)
                                    .map_err(failure::Error::from)
                            }))
                    }

                    None => Either::B(tokio::io::write_all(target, NO_LOG_STRING)
                                .map(|(target, _)| target)
                                .map_err(failure::Error::from)),
                };

                Either::A(ok_future)
            }

            Err(e) => Either::B(failed(
                format_err!("Unable to get start commit for log listing: {}", e)))
        }
    }

    fn commit_handler<T>(&self, message: String, target: T)
        -> impl Future<Item=T, Error=failure::Error>+Send
        where T: AsyncWrite+Send+Sync+'static {
        let message = match self.controller.write().unwrap().commit(message.as_str()) {
            Ok(cache_ref) => format!("Successfully committed, HEAD at {}", cache_ref),
            Err(e) => format!("Failed to commit: {}", e)
        };

        tokio::io::write_all(target, message)
            .map(|(target, _)| target)
            .map_err(failure::Error::from)
    }
}

pub fn execute_commands<W, C>(channel: C, executor: CommandExecutor<W>)
    -> impl Future<Item=(), Error=failure::Error> where W: for<'a> WorkspaceController<'a>+'static,
                                                        C: AsyncRead+AsyncWrite+Send+'static {
    let (input, output) = channel.split();
    tokio::io::read_to_end(input, Vec::new())
        .map_err(failure::Error::from)
        .and_then(|(_, command_string)|
                  serde_json::from_slice(&command_string[..])
                    .map_err(failure::Error::from))
        .and_then(move |command|
            executor.command(command, output))
        .and_then(|target|
            tokio::io::shutdown(target)
                .map(|_| ())
                .map_err(failure::Error::from))
        .then(|result| match result {
            Ok(()) => Ok(()),
            Err(e) => {
                error!("Error during command processing: {}", e);
                Ok(())
            }
        })
}

#[cfg(target_os = "linux")]
pub fn create_command_socket<P, W>(path: P, command_executor: CommandExecutor<W>)
    -> impl Future<Item=(), Error=failure::Error> where P: AsRef<Path>,
                                                        W: for<'a> WorkspaceController<'a> {
    debug!("Creating command socket at {}", path.as_ref().display());

    let listener = tokio_uds::UnixListener::bind(&path)
        .expect(format!("Unable to bind UDS listener {}", path.as_ref().display()).as_str());

    listener.incoming()
        .map_err(failure::Error::from)
        .for_each(move |connection|
            execute_commands(connection, command_executor.clone()))
}

#[cfg(target_os = "linux")]
fn find_command_stream() -> impl Future<Item=tokio_uds::UnixStream, Error=io::Error> {
    fn find_stream_path() -> io::Result<PathBuf> {
        let cwd = std::env::current_dir()?;

        let mut dork_root = cwd.as_path();
        if dork_root.ends_with(".dork") {
            dork_root = dork_root.parent().unwrap();
        }

        loop {
            debug!("Looking for command socket in {}", dork_root.display());

            let command_socket_path = dork_root.join(".dork/cmd");
            if command_socket_path.exists() {
                info!("Using {} as command socket path", command_socket_path.display());
                break Ok(command_socket_path);
            }

            match dork_root.parent() {
                Some(parent) => dork_root = parent,
                None => break Err(io::Error::new(
                        io::ErrorKind::AddrNotAvailable,
                        format!("Unable to find a mounted dork file system at {}",
                                cwd.display())))
            }
        }
    }

    find_stream_path().into_future()
        .and_then(|command_socket_path|
            tokio_uds::UnixStream::connect(command_socket_path))
}

#[cfg(not(target_os = "linux"))]
fn find_command_stream() -> impl Future<Item=tokio::fs::File, Error=io::Error> {
    futures::failed(io::Error::new(
        io::ErrorKind::Other,
        "dorkfs only supports Linux. Bummer."))
}

pub fn send_command(command: Command) {
    let command_string = serde_json::to_string(&command).unwrap();
    let task = find_command_stream()
        .and_then(move |stream| tokio::io::write_all(stream, command_string))
        .and_then(|(stream, _)| {
            let result = stream.shutdown(Shutdown::Write);
            result.map(move |_| stream)
        })
        .and_then(|stream| tokio::io::copy(stream, tokio::io::stdout()))
        .and_then(|(_, stream, _)| tokio::io::shutdown(stream))
        .map(|_| ())
        .map_err(|e| {
            error!("Unable to communicate with daemon: {}", e);
            ()
        });

    tokio::runtime::run(task);
}


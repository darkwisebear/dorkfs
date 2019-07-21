use std::{
    path::Path,
    sync::{Arc, RwLock}
};

use crate::{
    cache::CacheRef,
    overlay::WorkspaceController
};

use futures::{failed, Future, Stream, future::Either};
use tokio::{
    self,
    prelude::{AsyncRead, AsyncWrite}
};
use failure;
use serde_json;

#[derive(Deserialize)]
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

    pub fn command<T: AsyncWrite+Send+Sync+'static>(&self, command: Command, target: T) {
        let command_future = match command {
            Command::Commit { message } =>
                Box::new(self.commit_handler(message, target))
                    as Box<dyn Future<Item=(), Error=failure::Error>+Send>,

            Command::Log { start_commit } =>
                Box::new(self.handle_log_stream(start_commit, target))
                             as Box<dyn Future<Item=(), Error=failure::Error>+Send>
        };

        crate::tokio_runtime::get().executor().spawn(command_future
            .map_err(|e| {
                error!("Unable to handle command: {}", e);
                ()
            }));
    }

    fn handle_log_stream<T>(&self, start_commit: Option<CacheRef>, target: T)
        -> impl Future<Item=(), Error=failure::Error>+Send
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
        .and_then(|target|
            tokio::io::shutdown(target)
                .map(|_| ())
                .map_err(failure::Error::from))
    }

    fn commit_handler<T>(&self, message: String, target: T)
        -> impl Future<Item=(), Error=failure::Error>+Send
        where T: AsyncWrite+Send+Sync+'static {
        let message = match self.controller.write().unwrap().commit(message.as_str()) {
            Ok(cache_ref) => format!("Successfully committed, HEAD at {}", cache_ref),
            Err(e) => format!("Failed to commit: {}", e)
        };

        tokio::io::write_all(target, message)
            .map(|_| ())
            .map_err(failure::Error::from)
    }
}

pub fn parse_command<S: AsyncRead>(source: S) -> impl Future<Item=(S, Command), Error=failure::Error> {
    let buffer = Vec::with_capacity(256);
    tokio::io::read_to_end(source, buffer)
        .map_err(failure::Error::from)
        .and_then(|(source, buffer)| {
            serde_json::from_slice(buffer.as_slice())
                .map(move |c| (source, c))
                .map_err(failure::Error::from)
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
        .for_each(move |connection| {
            // We need to clone here since the closure of for_each
            // needs command_executor to be preserved in its state so
            // that it can be used on the next iteration again.
            let command_executor = command_executor.clone();
            parse_command(connection)
                .map(move |(connection, command)|
                    command_executor.command(command, connection))
        })
}

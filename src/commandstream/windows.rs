use std::{
    io::{self, Read},
    fs::File,
    path::Path,
    process,
};

use winpipe;

pub(super) fn create_communication_server() -> io::Result<(winpipe::Incoming, String)> {
    let pipe_name = format!("\\\\.\\pipe\\dorkfs-{}", process::id());
    let server = winpipe::NamedPipeServer::bind(&pipe_name);
    server.incoming()
        .map(|incoming| (incoming, pipe_name))
}

pub(super) async fn open_communication_channel(dorkdir: impl AsRef<Path>)
    -> io::Result<winpipe::PollNamedPipe> {
    let cmd_path = dorkdir.as_ref().join("cmd");
    let pipe_name = File::open(cmd_path)
        .and_then(|mut file| {
            let mut contents = String::new();
            file.read_to_string(&mut contents).map(|_| contents)
        })?;

    tokio::task::spawn_blocking(move ||
        winpipe::connect_named_pipe(pipe_name, Some(super::COMMAND_CHANNEL_CONNECT_TIMEOUT)))
        .await
        .unwrap()
}

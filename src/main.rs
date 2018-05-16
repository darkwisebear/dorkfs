extern crate clap;
#[macro_use] extern crate failure;
#[macro_use] extern crate failure_derive;
extern crate libc;
extern crate time;
#[macro_use] extern crate log;
extern crate env_logger;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate serde_json;
extern crate rand;
extern crate tiny_keccak;
#[macro_use] extern crate lazy_static;
extern crate tempfile;

#[cfg(feature = "fuse")]
extern crate fuse_mt;

#[cfg(feature = "fuse")]
mod fuse;
mod cache;
mod overlay;

use std::io::Write;
use std::path::{Path, PathBuf};

use failure::Error;

use cache::{CacheLayer, CacheRef, HashFileCache};
use overlay::Overlay;

fn parse_arguments() -> clap::ArgMatches<'static> {
    use clap::{App, Arg};

    App::new("dorkfs")
        .arg(Arg::with_name("cachedir")
            .takes_value(true)
            .required(true)
            .help("Directory where the cached contents shall be stored"))
        .arg(Arg::with_name("mountpoint")
            .takes_value(true)
            .required(true)
            .help("Mountpoint that shows the checked out contents"))
        .get_matches()
}

#[cfg(feature = "fuse")]
fn mount_fuse(mountpoint: &str, overlay: overlay::Overlay<cache::HashFileCache>) {
    let dorkfs = fuse::DorkFS::with_overlay(overlay).unwrap();
    dorkfs.mount(mountpoint).unwrap();
}

fn new_overlay<P: AsRef<Path>>(workspace: P) -> Result<Overlay<HashFileCache>, Error> {
    let cachedir = workspace.as_ref().join("cache");
    let overlaydir = workspace.as_ref().join("overlay");
    let cache = HashFileCache::new(&cachedir)?;
    Overlay::new(cache, overlaydir)
}

pub fn init_logging() {
    static INIT_LOGGING: std::sync::Once = std::sync::ONCE_INIT;
    INIT_LOGGING.call_once(|| env_logger::init());
}

fn main() {
    init_logging();

    let args = parse_arguments();
    let cachedir = args.value_of("cachedir").expect("cachedir arg not set!");
    let mountpoint = args.value_of("mountpoint").expect("mountpoint arg not set!");

    let fs = new_overlay(cachedir)
        .expect("Unable to create workspace");

    #[cfg(feature = "fuse")]
    mount_fuse(mountpoint, fs);
}

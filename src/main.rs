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
use std::path::PathBuf;
use cache::{CacheLayer, CacheRef};
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
fn mount_fuse(mountpoint: &str, cache: cache::HashFileCache, head_ref: cache::CacheRef) {
    let dorkfs = fuse::DorkFS::with_cache(cache, head_ref).unwrap();
    dorkfs.mount(mountpoint).unwrap();
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

    let base_dir = PathBuf::from(cachedir.to_owned());

    let cache = cache::HashFileCache::new(&base_dir.join("cache"))
        .expect("Unable to initialize cache");

    let mut overlay =
        Overlay::new(cache,
                     &base_dir.join("overlay"))
            .expect("Unable to instantiate overlay");
    let mut test_file = overlay.open_file("test.txt", true)
        .expect("Unable to create file");
    write!(test_file, "What a test!").expect("Couldn't write to test file");
    drop(test_file);

    let head_ref = overlay.commit("A test commit").expect("Unable to commit");

    #[cfg(feature = "fuse")]
    mount_fuse(mountpoint, cache, head_ref);
}

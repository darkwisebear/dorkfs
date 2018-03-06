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

#[cfg(feature = "fuse")]
extern crate fuse_mt;

#[cfg(feature = "fuse")]
mod fuse;
mod cache;

use cache::{WritableFile, WritableDirectory, CacheLayer};
use std::io::Write;

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

fn main() {
    env_logger::init();

    let args = parse_arguments();
    let cachedir = args.value_of("cachedir").expect("cachedir arg not set!");
    let mountpoint = args.value_of("mountpoint").expect("mountpoint arg not set!");

    let mut cache = cache::HashFileCache::new(&cachedir)
        .expect("Unable to initialize cache");

    let mut test_file = cache.create_file()
        .expect("Unable to create demo file");
    write!(test_file, "What a test!")
        .expect("Couldn't write to test file");
    let test_file_ref = test_file.finish()
        .and_then(|file| cache.add(cache::CacheObject::File(file)))
        .expect("Unable to add test file to cache");

    let mut test_dir = cache.create_directory()
        .expect("Unable to create root dir");
    let dir_entry = cache::DirectoryEntry {
        cache_ref: test_file_ref,
        object_type: cache::ObjectType::File,
        name: "test.txt".to_string()
    };
    test_dir.add_entry(dir_entry).expect("Unable to add test file to root directory");
    let root_dir_ref = test_dir.finish()
        .and_then(|dir| cache.add(cache::CacheObject::directory(dir)))
        .expect("Couldn't add root dir to cache");

    let commit = cache::Commit {
        tree: root_dir_ref,
        message: "A test commit".to_string(),
        parents: vec![cache::CacheRef::root()]
    };
    let head_ref = cache.add(cache::CacheObject::Commit(commit))
        .expect("Unable to add commit");

    #[cfg(feature = "fuse")]
    mount_fuse(mountpoint, cache, head_ref);
}

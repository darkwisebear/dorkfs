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
mod types;
mod utility;
mod control;

use std::io::Write;
use std::path::Path;
use std::str::FromStr;

use failure::Error;

use cache::HashFileCache;
use overlay::FilesystemOverlay;

fn is_octal_number(s: &str) -> bool {
    s.chars().all(|c| c >= '0' && c <='7')
}

fn validate_umask(s: String) -> Result<(), String> {
    if is_octal_number(s.as_str()) && s.len() == 3 {
        Ok(())
    } else {
        Err("Parameter must be a valid octal umask".to_string())
    }
}

fn parse_umask(s: &str) -> u32 {
    s.chars().fold(0u32, |v, c| {
        (v << 3u32) + c.to_digit(8).expect("Number is not an octal!")
    })
}

#[cfg(target_os="linux")]
fn resolve_uid(uid: &str) -> Result<u32, Error> {
    match u32::from_str(uid) {
        Ok(uid) => Ok(uid),
        Err(_) => {
            let name = std::ffi::CString::new(uid).unwrap();
            unsafe {
                let passwd = libc::getpwnam(name.as_ptr());
                if let Some(passwd) = passwd.as_ref() {
                    Ok(passwd.pw_uid)
                } else {
                    bail!("Unable to resolve the uid");
                }
            }
        }
    }
}

#[cfg(not(target_os="linux"))]
fn resolve_uid(uid: &str) -> Result<u32, Error> {
    u32::from_str(uid).map_err(|e| e.into())
}

#[cfg(target_os="linux")]
fn resolve_gid(gid: &str) -> Result<u32, Error> {
    match u32::from_str(gid) {
        Ok(gid) => Ok(gid),
        Err(_) => {
            let name = std::ffi::CString::new(gid).unwrap();
            unsafe {
                let group = libc::getgrnam(name.as_ptr());
                if let Some(group) = group.as_ref() {
                    Ok(group.gr_gid)
                } else {
                    bail!("Unable to resolve the gid");
                }
            }
        }
    }
}

#[cfg(not(target_os="linux"))]
fn resolve_gid(gid: &str) -> Result<u32, Error> {
    u32::from_str(gid).map_err(|e| e.into())
}

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
        .arg(Arg::with_name("uid")
            .takes_value(true)
            .default_value("0")
            .long("uid"))
        .arg(Arg::with_name("gid")
            .takes_value(true)
            .default_value("0")
            .long("gid"))
        .arg(Arg::with_name("umask")
            .takes_value(true)
            .default_value("022")
            .validator(validate_umask)
            .long("umask"))
        .get_matches()
}

#[cfg(feature = "fuse")]
fn mount_fuse(mountpoint: &str,
              overlay: overlay::FilesystemOverlay<cache::HashFileCache>,
              uid: u32,
              gid: u32,
              umask: u32) {
    let dorkfs = fuse::DorkFS::with_overlay(overlay, uid, gid, umask).unwrap();
    dorkfs.mount(mountpoint).unwrap();
}

fn new_overlay<P: AsRef<Path>>(workspace: P) -> Result<FilesystemOverlay<HashFileCache>, Error> {
    let cachedir = workspace.as_ref().join("cache");
    let overlaydir = workspace.as_ref().join("overlay");
    let cache = HashFileCache::new(cachedir)?;
    FilesystemOverlay::new(cache, overlaydir)
}

pub fn init_logging() {
    static INIT_LOGGING: std::sync::Once = std::sync::ONCE_INIT;
    INIT_LOGGING.call_once(|| env_logger::init());
}

fn main() {
    init_logging();

    let args = parse_arguments();
    let cachedir = args.value_of("cachedir").expect("cachedir arg not set!");

    let fs = new_overlay(cachedir)
        .expect("Unable to create workspace");

    #[cfg(feature = "fuse")]
        {
            let mountpoint = args.value_of("mountpoint").expect("mountpoint arg not set!");
            let umask = args.value_of("umask")
                .map(parse_umask)
                .expect("Unparsable umask");
            let uid = resolve_uid(args.value_of("uid").unwrap())
                .expect("Cannot parse UID");
            let gid = resolve_gid(args.value_of("gid").unwrap())
                .expect("Cannot parse GID");

            mount_fuse(mountpoint, fs, uid, gid, umask);
        }
}

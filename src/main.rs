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
extern crate hyper;
extern crate hyper_tls;
extern crate http;
extern crate futures;
extern crate tokio;
extern crate base64;
extern crate bytes;

#[cfg(target_os = "linux")]
extern crate fuse_mt;

#[cfg(target_os = "linux")]
mod fuse;
#[cfg(feature = "gitcache")]
mod gitcache;
mod cache;
mod overlay;
mod types;
mod utility;
mod control;
mod hashfilecache;
mod github;
#[cfg(test)]
mod nullcache;

use std::path::Path;
use std::str::FromStr;
use std::fmt::Debug;
use std::borrow::Cow;
use std::ffi::CString;

use failure::Fallible;

use crate::hashfilecache::HashFileCache;
use crate::overlay::FilesystemOverlay;
use crate::cache::CacheLayer;
use crate::utility::RootrepoUrl;

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

fn parse_umask(s: &str) -> u16 {
    s.chars().fold(0u16, |v, c| {
        (v << 3u16) + c.to_digit(8).expect("Number is not an octal!") as u16
    })
}

#[cfg(target_os="linux")]
fn resolve_uid(uid: Option<&str>) -> Fallible<u32> {
    match uid {
        Some(uid) => match u32::from_str(uid) {
            Ok(uid) => Ok(uid),
            Err(_) => {
                let name = CString::new(uid).unwrap();
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

        None => unsafe {
            Ok(libc::geteuid())
        }
    }
}

#[cfg(target_os="linux")]
fn resolve_gid(gid: Option<&str>) -> Fallible<u32> {
    match gid {
        Some(gid) => match u32::from_str(gid) {
            Ok(gid) => Ok(gid),
            Err(_) => {
                let name = CString::new(gid).unwrap();
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

        None => unsafe {
            Ok(libc::getegid())
        }
    }
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
        .arg(Arg::with_name("rootrepo")
            .takes_value(true)
            .required(true)
            .help("Connection specification to the root repository. For GitHub this string has \
            the following form: github+<GitHub API URL>/<org>/<repo>"))
        .arg(Arg::with_name("uid")
            .takes_value(true)
            .long("uid")
            .help("UID to be used for the files. Defaults to the effective UID of the process."))
        .arg(Arg::with_name("gid")
            .takes_value(true)
            .long("gid")
            .help("GID to be used for the files. Defaults to the effective GID of the process."))
        .arg(Arg::with_name("umask")
            .takes_value(true)
            .default_value("022")
            .validator(validate_umask)
            .long("umask"))
        .arg(Arg::with_name("branch")
            .takes_value(true)
            .long("branch")
            .short("b")
            .help("Remote branch that shall be tracked instead of the default branch"))
        .get_matches()
}

#[cfg(target_os = "linux")]
fn mount_fuse<C>(
    mountpoint: &str,
    overlay: overlay::FilesystemOverlay<C>,
    uid: u32,
    gid: u32,
    umask: u16)
    where C: CacheLayer+Debug+'static+Send+Sync,
          <C as CacheLayer>::File: 'static+Send+Sync {
    let dorkfs = fuse::DorkFS::with_overlay(overlay, uid, gid, umask).unwrap();
    dorkfs.mount(mountpoint).unwrap();
}

fn new_overlay<P, U, B>(workspace: P, rooturl: U, branch: Option<B>)
    -> Fallible<FilesystemOverlay<cache::BoxedCacheLayer>>
    where P: AsRef<Path>,
          U: AsRef<str>,
          B: AsRef<str> {
    let overlaydir = workspace.as_ref().join("overlay");
    let cachedir = workspace.as_ref().join("cache");

    let baseurl = rooturl.as_ref();
    let rootrepo_url = RootrepoUrl::from_str(baseurl)?;
    let (rootrepo, branch) = match rootrepo_url {
        RootrepoUrl::GithubHttps { apiurl, org, repo } => {
            let mut baseurl = String::from("https://");
            baseurl.push_str(apiurl);
            let token = ::std::env::var("GITHUB_TOKEN")
                .expect("GITHUB_TOKEN needed in order to authenticate against GitHub");
            debug!("Connecting to GitHub at {} org {} repo {}", &baseurl, org, repo);
            let github = github::Github::new(
                baseurl.as_str(),
                org,
                repo,
                token.as_str())?;

            let branch = branch.as_ref().map(|s| Cow::Borrowed(s.as_ref()))
                .unwrap_or_else(|| Cow::Owned(github.get_default_branch().unwrap()));

            let cached_github =
                cache::boxed(HashFileCache::new(github, cachedir)?);

            (cached_github, branch)
        }
    };

    FilesystemOverlay::new(rootrepo, overlaydir, branch.as_ref())
        .map_err(Into::into)
}

pub fn init_logging() {
    static INIT_LOGGING: std::sync::Once = std::sync::ONCE_INIT;
    INIT_LOGGING.call_once(|| env_logger::init());
}

fn main() {
    init_logging();

    let args = parse_arguments();
    let cachedir = args.value_of("cachedir").expect("cachedir arg not set!");
    let rootrepo = args.value_of("rootrepo").expect("No root URL given");
    let branch = args.value_of("branch");

    let fs = new_overlay(cachedir, rootrepo, branch)
        .expect("Unable to create workspace");

    #[cfg(target_os = "linux")]
    {
        let mountpoint = args.value_of("mountpoint").expect("mountpoint arg not set!");
        let umask = args.value_of("umask")
            .map(parse_umask)
            .expect("Unparsable umask");
        let uid = resolve_uid(args.value_of("uid"))
            .expect("Cannot parse UID");
        let gid = resolve_gid(args.value_of("gid"))
            .expect("Cannot parse GID");

        mount_fuse(mountpoint, fs, uid, gid, umask);
    }
}

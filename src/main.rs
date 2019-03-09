#![allow(clippy::unneeded_field_pattern, clippy::new_ret_no_self)]

extern crate clap;
#[macro_use] extern crate failure;
#[macro_use] extern crate failure_derive;
extern crate libc;
extern crate chrono;
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
extern crate regex;

#[cfg(target_os = "linux")]
extern crate fuse_mt;

#[cfg(target_os = "linux")]
mod fuse;
#[cfg(feature = "gitcache")]
mod gitcache;
mod cache;
mod dispatch;
mod overlay;
mod types;
mod utility;
mod control;
mod hashfilecache;
mod github;
#[cfg(test)]
mod nullcache;

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::fmt::Debug;
use std::borrow::Cow;
use std::ffi::CString;
use std::iter;

use failure::Fallible;
use clap::ArgMatches;

use crate::{
    hashfilecache::HashFileCache,
    overlay::{WorkspaceController, FilesystemOverlay},
    cache::CacheLayer,
    utility::RepoUrl,
    types::RepoRef
};
use crate::overlay::Overlay;
use crate::cache::CacheRef;
use crate::dispatch::PathDispatcher;
use crate::control::ControlDir;

fn is_octal_number(s: &str) -> bool {
    s.chars().all(|c| c >= '0' && c <='7')
}

fn validate_umask<S: AsRef<str>>(s: S) -> Result<(), String> {
    let s = s.as_ref();
    if is_octal_number(s) && s.len() == 3 {
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

    let mount = App::new("mount")
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
            the following form: github+<GitHub URL>/<org>/<repo>"))
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
            .help("Remote branch that shall be tracked instead of the default branch"));

    let dorkfs = App::new("dorkfs")
        .subcommand(mount);

    dorkfs.get_matches()
}

#[cfg(target_os = "linux")]
fn mount_fuse<C>(
    mountpoint: &str,
    dispatcher: PathDispatcher<ControlDir<FilesystemOverlay<C>>>,
    uid: u32,
    gid: u32,
    umask: u16)
    where C: CacheLayer+Debug+'static+Send+Sync,
          <C as CacheLayer>::File: 'static+Send+Sync {
    let dorkfs = fuse::DorkFS::with_path_dispatcher(dispatcher, uid, gid, umask).unwrap();
    dorkfs.mount(mountpoint).unwrap();
}

fn new_overlay<P, Q>(overlaydir: P, cachedir: Q, rootrepo_url: &RepoUrl, branch: Option<&RepoRef>)
    -> Fallible<FilesystemOverlay<cache::BoxedCacheLayer>>
    where P: AsRef<Path>,
          Q: AsRef<Path> {
    let mut default_branch = String::new();

    let (rootrepo, branch) = match rootrepo_url {
        RepoUrl::GithubHttps { apiurl, org, repo } => {
            let baseurl = format!("https://{}", apiurl);
            let token = ::std::env::var("GITHUB_TOKEN")
                .expect("GITHUB_TOKEN needed in order to authenticate against GitHub");
            debug!("Connecting to GitHub at {} org {} repo {}", &baseurl, org, repo);
            let github = github::Github::new(
                baseurl.as_str(),
                &org,
                &repo,
                token.as_str())?;

            let branch = match branch {
                Some(&RepoRef::Branch(branch)) => RepoRef::Branch(branch),
                Some(&RepoRef::CacheRef(cache_ref)) => RepoRef::CacheRef(cache_ref),
                None => {
                    default_branch = github.get_default_branch()?;
                    RepoRef::Branch(default_branch.as_str())
                }
            };

            let cached_github =
                HashFileCache::new(github, &cachedir)?;

            (cache::boxed(cached_github), branch)
        }
    };

    FilesystemOverlay::new(rootrepo,
                           Cow::Borrowed(overlaydir.as_ref()),
                           branch)
        .and_then(|mut overlay|
            match overlay.update_head() {
                Err(overlay::Error::UncleanWorkspace) => {
                    info!("Not updating current workspace since it is unclean.");
                    Ok(overlay)
                }

                Err(overlay::Error::DetachedHead) => {
                    info!("Not updating workspace since HEAD is detached.");
                    Ok(overlay)
                }

                Err(err) => Err(err),

                Ok(cache_ref) => {
                    println!("Updating workspace to latest HEAD of {} -> {}",
                             overlay.get_current_branch()?
                                 .expect("No tracked branch but update worked..?"),
                             &cache_ref);
                    Ok(overlay)
                }
            }
        )
        .map_err(Into::into)
}

fn get_commit<C: CacheLayer>(cache: &C, commit_ref: &CacheRef) -> Fallible<cache::Commit> {
    cache.get(commit_ref)
        .and_then(|obj| obj.into_commit())
        .map_err(failure::Error::from)
}

pub fn init_logging() {
    static INIT_LOGGING: std::sync::Once = std::sync::ONCE_INIT;
    INIT_LOGGING.call_once(env_logger::init);
}

fn mount(args: &ArgMatches) {
    let cachedir_base = Path::new(args.value_of("cachedir")
        .expect("cachedir arg not set!"));
    let rootrepo = args.value_of("rootrepo").expect("No root URL given");
    let branch = args.value_of("branch").map(RepoRef::Branch);

    let rootrepo_url = RepoUrl::from_str(rootrepo).expect("Unable to parse root URL");
    let cachedir = cachedir_base.join("cache");

    let mut fs = new_overlay(cachedir_base.join("overlay/root"),
                             &cachedir,
                             &rootrepo_url,
                             branch.as_ref())
        .expect("Unable to create workspace");

    let mut dispatcher = dispatch::PathDispatcher::new();

    let head_commit = fs.get_current_head_ref().transpose()
        .map(|cache_ref|
            cache_ref
                .map_err(failure::Error::from)
                .and_then(|cache_ref|
                    get_commit(fs.get_cache(), &cache_ref)))
        .transpose()
        .expect("Unable to retrieve HEAD commit");

    if let Some(head_commit) = head_commit {
        match fs.open_file(".gitmodules", false) {
            Ok(file) => {
                let workspace_dir = cachedir_base.join("overlay/submodules");

                match rootrepo_url {
                    RepoUrl::GithubHttps {
                        apiurl, org, ..
                    } => {
                        match github::initialize_github_submodules(
                            file, apiurl.as_ref(), org.as_ref()) {
                            Ok(submodules) => {
                                for (submodule_path, submodule_url) in submodules.into_iter() {
                                    let cache = fs.get_cache().as_ref();
                                    let gitlink_ref = cache::resolve_object_ref(cache, &head_commit, &submodule_path)
                                        .map_err(failure::Error::from)
                                        .and_then(|cache_ref|
                                            cache_ref.ok_or(format_err!(
                                            "Unable to find gitlink for submodule in path {}",
                                            submodule_path.display())));
                                    match gitlink_ref {
                                        Ok(gitlink_ref) => {
                                            new_overlay(workspace_dir.join(&submodule_path),
                                                        &cachedir,
                                                        &submodule_url,
                                                        Some(&RepoRef::CacheRef(gitlink_ref)))
                                                .and_then(|submodule|
                                                    dispatcher.add_overlay(
                                                        submodule_path,
                                                        ControlDir::new(submodule)))
                                                .expect("Unable to create submodule workspace");
                                        }

                                        Err(err) =>
                                            warn!("Unable to initialize submodule: {}. Skipping.",
                                                  err)
                                    }
                                }
                            }

                            Err(err) => warn!("Unable to initialize submodules: {}", err)
                        }
                    }
                }
            }

            Err(overlay::Error::FileNotFound) => debug!("No .gitmodules found, skipping submodule scan"),
            Err(err) => warn!("Unable to open .gitmodules: {}", err)
        }
    }

    dispatcher.add_overlay("", ControlDir::new(fs))
        .expect("Unable to mount root repository");

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

        mount_fuse(mountpoint, dispatcher, uid, gid, umask);
    }
}

fn main() {
    init_logging();

    let args = parse_arguments();
    match args.subcommand() {
        ("mount", Some(subargs)) => mount(subargs),
        _ => print!("{}", args.usage())
    }
}

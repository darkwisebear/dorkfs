#![allow(clippy::unneeded_field_pattern, clippy::new_ret_no_self)]

#[macro_use] extern crate clap;
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
extern crate owning_ref;
extern crate either;
extern crate glob;
extern crate lru;

#[cfg(target_os = "linux")]
extern crate fuse_mt;
#[cfg(target_os = "linux")]
extern crate tokio_uds;

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
mod commandstream;

mod tokio_runtime {
    use std::{
        sync::{
            mpsc::channel,
            Mutex,
            Weak,
            Arc
        },
        time::Duration
    };

    use tokio::{
        prelude::*,
        runtime::Runtime,
        timer::timeout::Error as TimeoutError
    };

    lazy_static! {
        static ref TOKIO_RUNTIME: Mutex<Weak<Runtime>> = Mutex::new(Weak::new());
    }

    pub fn get() -> Arc<Runtime> {
        let mut runtime = TOKIO_RUNTIME.lock().unwrap();
        match runtime.upgrade() {
            Some(tokio) => tokio,
            None => {
                let new_runtime = Runtime::new().expect("Unable to instantiate tokio runtime");
                let new_runtime = Arc::new(new_runtime);
                *runtime = Arc::downgrade(&new_runtime);
                new_runtime
            }
        }
    }

    pub fn execute<T, E, I, F>(tokio: &Runtime, request_future: I, timeout: Duration)
        -> Result<T, TimeoutError<E>>
        where T: Send+'static,
              E: Send+'static,
              I: IntoFuture<Future=F, Item=T, Error=E>+Send+'static,
              F: Future<Item=T, Error=E>+Send+'static {
        let (send, recv) = channel();

        let finalizing_future = request_future
            .into_future()
            .timeout(timeout)
            .then(move |r| send.send(r)
                .map_err(|_| unreachable!("Unexpected send error during GitHub request")));

        tokio.executor().spawn(finalizing_future);
        recv.recv().expect("Unexpected receive error during GitHub request")
    }
}

use std::path::Path;
use std::str::FromStr;
use std::fmt::Debug;
use std::borrow::Cow;
use std::ffi::CString;
use std::io::Read;

use failure::Fallible;
use clap::ArgMatches;

use crate::{
    hashfilecache::HashFileCache,
    overlay::{WorkspaceController, FilesystemOverlay, BoxedRepository, RepositoryWrapper, Overlay},
    cache::{CacheLayer, CacheRef},
    control::ControlDir,
    utility::{RepoUrl, CommitRange},
    types::RepoRef
};

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
    use clap::{App, Arg, SubCommand};

    let mount = SubCommand::with_name("mount")
        .about("Mounts the given repository at the given directory using the given temporary file \
        path for internal metadata and state")
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

    let log = SubCommand::with_name("log")
        .about("Prints the commit log history of the repository that is mounted at the current \
        working directory")
        .arg(Arg::with_name("commit_range")
            .default_value("..")
            .help("Specify the commit range that is being displayed. Currently, only the \
            full range or a start commit can be specified. The full range looks like this: \"..\". \
            With a start commit, the range looks like this: \"<start_commit>..\""));

    let switch = SubCommand::with_name("switch")
        .about("Switch to the given target branch")
        .arg(Arg::with_name("target_branch")
            .required(true)
            .help("Switch to the given branch. All overlay files will stay as-is."));

    let branch = SubCommand::with_name("branch")
        .about("Print the current branch or create a new branch at the current HEAD of the current \
        branch")
        .arg(Arg::with_name("new_branch")
            .help("When given, create a branch with this name that starts at HEAD of the \
            current branch"));

    let revert = SubCommand::with_name("revert")
        .about("Revert the set of files that match a given glob expression")
        .arg(Arg::with_name("revert_glob")
                 .default_value("*")
                 .help("Specify a glob that tells which files shall be reverted"))
        .arg(Arg::with_name("dry_run")
                 .long("dry-run"));

    let status = SubCommand::with_name("status")
        .about("Displays the current workspace status which is a list of files that have been \
        added/deleted/modified");

    let update = SubCommand::with_name("update")
        .about("Update the working area to the HEAD revision of the server. Equivalent to \
        calling \"branch\" without arguments");

    let commit = SubCommand::with_name("commit")
        .about("Create a new commit with all changes that have been done inside the mounted \
        repository")
        .arg(Arg::with_name("message")
             .short("m")
             .help("Use the given message as the message for the newly created commit")
             .takes_value(true)
             .required(true));

    let dorkfs = App::new("dorkfs")
        .version(crate_version!())
        .about("Mount git repositories and work with them just as you would with an ordinary disk \
        drive")
        .subcommand(mount)
        .subcommand(log)
        .subcommand(switch)
        .subcommand(branch)
        .subcommand(revert)
        .subcommand(status)
        .subcommand(commit)
        .subcommand(update);

    dorkfs.get_matches()
}

#[cfg(target_os = "linux")]
fn mount_fuse<O>(
    mountpoint: &str,
    root_overlay: O,
    uid: u32,
    gid: u32,
    umask: u16)
    where O: Overlay+Debug+Send+Sync+'static,
          <O as Overlay>::File: Debug+Send+Sync+'static {
    fuse::DorkFS::with_overlay(root_overlay, uid, gid, umask)
        .and_then(|dorkfs| dorkfs.mount(mountpoint))
        .expect("Unable to mount filesystem");
}

fn mount_submodules<R, P, Q, C>(rootrepo_url: &RepoUrl,
                                gitmodules: R,
                                cachedir: P,
                                workspace_dir: Q,
                                fs: &mut FilesystemOverlay<C>) -> Fallible<()>
    where R: Read,
          P: AsRef<Path>,
          Q: AsRef<Path>,
          C: CacheLayer+Debug+Send+Sync+'static,
          <C as CacheLayer>::GetFuture: Send {
    let head_commit = match fs.get_current_head_ref()? {
        Some(head_commit) => {
            let cache = fs.get_cache();
            get_commit(&*cache, &head_commit)?
        }
        None => return Ok(()),
    };

    match rootrepo_url {
        RepoUrl::GithubHttps {
            apiurl, org, ..
        } => match github::initialize_github_submodules(
            gitmodules, apiurl.as_ref(), org.as_ref()) {
            Ok(submodules) => {
                for (submodule_path, submodule_url) in submodules.into_iter() {
                    let cache = fs.get_cache();
                    let gitlink_ref =
                        cache::resolve_object_ref(cache, &head_commit, &submodule_path)
                            .map_err(failure::Error::from)
                            .and_then(|cache_ref|
                                cache_ref.ok_or_else(||
                                    format_err!("Unable to find gitlink for submodule in path {}",
                                                    submodule_path.display())));
                    match gitlink_ref {
                        Ok(gitlink_ref) =>
                            new_overlay(workspace_dir.as_ref().join(&submodule_path),
                                        cachedir.as_ref(),
                                        &submodule_url,
                                        Some(&RepoRef::CacheRef(gitlink_ref)))
                                .and_then(|submodule|
                                    fs.add_submodule(submodule_path, submodule)
                                        .map_err(Into::into))?,

                        Err(err) =>
                            error!("Unable to initialize submodule at {}: {}. Skipping.",
                                   submodule_path.display(), err)
                    }
                }
            }

            Err(err) => warn!("Unable to initialize submodules: {}", err)
        }
    }

    Ok(())
}

fn new_overlay<P, Q>(overlaydir: P, cachedir: Q, rootrepo_url: &RepoUrl, branch: Option<&RepoRef>)
    -> Fallible<BoxedRepository>
    where P: AsRef<Path>,
          Q: AsRef<Path> {
    let default_branch;
    let overlaydir = overlaydir.as_ref();

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

            (cached_github, branch)
        }
    };

    let tempdir = tempfile::tempdir()?;

    FilesystemOverlay::new(rootrepo,
                           Cow::Owned(overlaydir.join("root")),
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
        .and_then(|mut fs| {
            match fs.open_file(Path::new(".gitmodules"), false) {
                Ok(file) => {
                    let submodule_dir = overlaydir.join("submodules");
                    mount_submodules(rootrepo_url,
                                     file,
                                     cachedir.as_ref(),
                                     submodule_dir,
                                     &mut fs)?;
                }

                Err(overlay::Error::FileNotFound) => debug!("No .gitmodules found, skipping submodule scan"),
                Err(err) => warn!("Unable to open .gitmodules: {}", err)
            }

            Ok(fs)
        })
        .map(move |overlay| {
            let control_dir = ControlDir::new(overlay, tempdir);
            Box::new(RepositoryWrapper::new(control_dir)) as BoxedRepository
        })
        .map_err(Into::into)
}

fn get_commit<C: CacheLayer>(cache: &C, commit_ref: &CacheRef) -> Fallible<cache::Commit> {
    cache.get(commit_ref)
        .and_then(|obj| obj.into_commit())
        .map_err(failure::Error::from)
}

pub fn init_logging() {
    static INIT_LOGGING: std::sync::Once = std::sync::Once::new();
    INIT_LOGGING.call_once(env_logger::init);
}

fn mount(args: &ArgMatches) {
    let cachedir_base = Path::new(args.value_of("cachedir")
        .expect("cachedir arg not set!"));
    let rootrepo = args.value_of("rootrepo").expect("No root URL given");
    let branch = args.value_of("branch").map(RepoRef::Branch);

    let rootrepo_url = RepoUrl::from_str(rootrepo).expect("Unable to parse root URL");
    let cachedir = cachedir_base.join("cache");

    let fs = new_overlay(cachedir_base.join("overlay"),
                         &cachedir,
                         &rootrepo_url,
                         branch.as_ref())
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

fn log(args: &ArgMatches) {
    let range = CommitRange::from_str(args.value_of("commit_range")
        .unwrap())
        .expect("Unparsable commit range");

    let start = match range {
        CommitRange {
            start,
            end: None
        } => start,

        _ => unimplemented!("Only unbounded and ranges with only a start supported")
    };

    let start_commit = start
        .map(|s| cache::CacheRef::from_str(s)
            .expect("Unable to parse start ref in range"));

    let log_command = commandstream::Command::Log { start_commit };
    commandstream::send_command(log_command);
}

fn main() {
    init_logging();

    let args = parse_arguments();
    match args.subcommand() {
        ("mount", Some(subargs)) => mount(subargs),
        ("log", Some(subargs)) => log(subargs),
        ("status", Some(_)) =>
            commandstream::send_command(commandstream::Command::Status),
        ("branch", Some(subargs)) => {
            let command = match subargs.value_of("new_branch") {
                Some(target_branch_name) => commandstream::Command::CreateBranch {
                    name: target_branch_name.to_string()
                },

                None => commandstream::Command::CurrentBranch {
                    target_branch: None
                }
            };

            commandstream::send_command(command)
        }
        ("switch", Some(subargs)) => {
            let target_branch = subargs.value_of("target_branch").unwrap();
            commandstream::send_command(
                commandstream::Command::CurrentBranch {
                    target_branch: Some(target_branch.to_string())
                })
        }
        ("revert", Some(subargs)) => {
            let path_glob = subargs.value_of("revert_glob").unwrap().to_string();
            let dry_run = subargs.is_present("dry_run");
            let command = commandstream::Command::Revert {
                path_glob, dry_run
            };

            commandstream::send_command(command)
        }
        ("update", Some(_)) => {
            commandstream::send_command(commandstream::Command::CurrentBranch {
                target_branch: None
            })
        }
        ("commit", Some(subargs)) => {
            commandstream::send_command(commandstream::Command::Commit {
                message: subargs.value_of("message").unwrap().to_string()
            })
        }
        _ => unreachable!()
    }
}

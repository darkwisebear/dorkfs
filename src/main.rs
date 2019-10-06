#![allow(clippy::unneeded_field_pattern, clippy::new_ret_no_self)]

#[macro_use] extern crate failure;
#[macro_use] extern crate failure_derive;
extern crate libc;
extern crate chrono;
#[macro_use] extern crate log;
extern crate env_logger;
extern crate serde;
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

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::fmt::Debug;
use std::borrow::Cow;
use std::io::Read;

use failure::Fallible;
use structopt::StructOpt;
use either::Either;

use crate::{
    hashfilecache::HashFileCache,
    overlay::{WorkspaceController, FilesystemOverlay, BoxedRepository, RepositoryWrapper, Overlay},
    cache::{CacheLayer, CacheRef, CommitRange},
    control::ControlDir,
    utility::RepoUrl,
    types::RepoRef,
};

#[cfg(not(target_os="linux"))]
mod uidgid {
    use std::str::FromStr;
    use std::num::ParseIntError;

    #[derive(Default)]
    pub struct Uid(u32);

    impl Uid {
        pub fn get(&self) -> u32 {
            self.0
        }
    }

    impl FromStr for Uid {
        type Err = ParseIntError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            u32::from_str(s).map(Uid)
        }
    }

    #[derive(Default)]
    pub struct Gid(u32);

    impl Gid {
        pub fn get(&self) -> u32 {
            self.0
        }
    }

    impl FromStr for Gid {
        type Err = ParseIntError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            u32::from_str(s).map(Gid)
        }
    }
}

#[cfg(target_os="linux")]
mod uidgid {
    use std::str::FromStr;
    use std::ffi::CString;

    use failure;
    use libc;

    pub struct Uid(u32);

    impl Uid {
        pub fn get(&self) -> u32 {
            self.0
        }
    }

    impl FromStr for Uid {
        type Err = failure::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            match u32::from_str(s) {
                Ok(uid) => Ok(Uid(uid)),
                Err(_) => {
                    let name = CString::new(s).unwrap();
                    unsafe {
                        let passwd = libc::getpwnam(name.as_ptr());
                        if let Some(passwd) = passwd.as_ref() {
                            Ok(Uid(passwd.pw_uid))
                        } else {
                            bail!("Unable to resolve uid {}", s);
                        }
                    }
                }
            }
        }
    }

    impl Default for Uid {
        fn default() -> Self {
            unsafe {
                Self(libc::geteuid())
            }
        }
    }

    pub struct Gid(u32);

    impl Gid {
        pub fn get(&self) -> u32 {
            self.0
        }
    }

    impl FromStr for Gid {
        type Err = failure::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            match u32::from_str(s) {
                Ok(gid) => Ok(Gid(gid)),
                Err(_) => {
                    let name = CString::new(s).unwrap();
                    unsafe {
                        let group = libc::getgrnam(name.as_ptr());
                        if let Some(group) = group.as_ref() {
                            Ok(Gid(group.gr_gid))
                        } else {
                            bail!("Unable to resolve gid {}", s);
                        }
                    }
                }
            }
        }
    }

    impl Default for Gid {
        fn default() -> Self {
            unsafe {
                Self(libc::getegid())
            }
        }
    }
}

use uidgid::{Uid, Gid};

struct UMask(u16);

impl FromStr for UMask {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.chars().fold(Ok(0u16), |v, c|
            v.and_then(|v| Ok((v << 3u16) + c.to_digit(8)
                .ok_or(format_err!("Unable to convert {} to a number", c))? as u16)))
        .map(UMask)
    }
}

impl UMask {
    fn get(&self) -> u16 {
        self.0
    }
}

#[derive(StructOpt)]
/// Mounts the given repository at the given directory using the given temporary file path for
/// internal metadata and state.
struct MountArguments {
    #[structopt(parse(from_os_str))]
    /// Directory where the cached contents shall be stored.
    cachedir: PathBuf,
    #[structopt(parse(from_os_str))]
    /// Mountpoint that shows the checked out contents.
    mountpoint: PathBuf,
    /// Connection specification to the root repository.
    ///
    /// For GitHub this string has the following form: github+<GitHub URL>/<org>/<repo>
    /// For a local git database storage, this string has the following form: git+file://<path>
    rootrepo: RepoUrl<'static>,
    #[structopt(long)]
    /// UID to be used for the files. Defaults to the effective UID of the process.
    uid: Option<Uid>,
    #[structopt(long)]
    /// GID to be used for the files. Defaults to the effective GID of the process.
    gid: Option<Gid>,
    #[structopt(long, default_value = "022")]
    /// Umask applied to all relevant file operations.
    umask: UMask,
    #[structopt(short, long)]
    /// Remote branch that shall be tracked instead of the default branch.
    branch: Option<String>
}

#[derive(StructOpt)]
/// Mount git repositories and work with them just as you would with an ordinary disk drive.
enum Arguments {
    Mount(MountArguments),
    /// Prints the commit log history of the repository that is mounted at the current
    /// working directory.
    Log {
        #[structopt(default_value = "..")]
        /// Specify the commit range that is being displayed.
        ///
        /// Currently, only the full range or a start commit can be specified. The full range looks
        /// like this: "..". With a start commit, the range looks like this: "<start_commit>..".
        commit_range: CommitRange
    },
    /// Switch to the given target branch.
    Switch {
        /// Switch to the given branch. All overlay files will stay as-is.
        target_branch: String
    },
    /// Print the current branch or create a new branch at the current HEAD of the current branch.
    Branch {
        /// When given, create a branch with this name that starts at HEAD of the current branch.
        /// Otherwise, the command prints the current branch to stdout.
        new_branch: Option<String>
    },
    /// Revert the set of files that match a given glob expression.
    Revert {
        #[structopt(default_value = "*")]
        /// Specify a glob that tells which files shall be reverted
        revert_glob: String,
        #[structopt(long)]
        /// If specified, the revert will just print what it would revert without touching the
        /// workspace at all.
        dry_run: bool
    },
    /// Displays the current workspace status which is a list of files that have been
    /// added/deleted/modified
    Status,
    /// Update the working area to the HEAD revision of the server. Equivalent to calling "branch"
    /// without arguments.
    Update,
    /// Create a new commit with all changes that have been done inside the mounted repository.
    Commit {
        #[structopt(short, long)]
        /// Use the given message as the message for the newly created commit.
        message: String
    }
}

#[cfg(target_os = "linux")]
fn mount_fuse<O, P>(
    mountpoint: P,
    root_overlay: O,
    uid: u32,
    gid: u32,
    umask: u16)
    where O: Overlay+Debug+Send+Sync+'static,
          <O as Overlay>::File: Debug+Send+Sync+'static,
          P: AsRef<Path> {
    fuse::DorkFS::with_overlay(root_overlay, uid, gid, umask)
        .and_then(move |dorkfs| dorkfs.mount(mountpoint))
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

        RepoUrl::GitFile { .. } => unimplemented!("Submodules not yet supported for local git storage")
    }

    Ok(())
}

#[cfg(feature = "gitcache")]
type MaybeGitCache = crate::gitcache::GitCache;
#[cfg(not(feature = "gitcache"))]
type MaybeGitCache = ();

fn new_overlay<P, Q>(overlaydir: P, cachedir: Q, rootrepo_url: &RepoUrl, branch: Option<&RepoRef>)
    -> Fallible<BoxedRepository>
    where P: AsRef<Path>,
          Q: AsRef<Path> {
    let default_branch;
    let overlaydir = overlaydir.as_ref();

    let (rootrepo, branch): (Either<_, MaybeGitCache>, _) = match rootrepo_url {
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
                Either::Left(HashFileCache::new(github, &cachedir)?);

            (cached_github, branch)
        }

        #[allow(unused_variables)] 
        RepoUrl::GitFile { path } => {
            #[cfg(feature = "gitcache")] {
                (Either::Right(crate::gitcache::GitCache::open(path)?), RepoRef::Branch("master"))
            }
            #[cfg(not(feature = "gitcache"))] {
                bail!("Log git repositories not enabled in this build!")
            }
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

fn mount(args: MountArguments) {
    let MountArguments {
        cachedir: cachedir_base,
        rootrepo: rootrepo_url,
        branch,
        mountpoint,
        uid,
        gid,
        umask
    } = args;

    let branch = branch.as_ref().map(|s| RepoRef::Branch(s.as_str()));

    let cachedir = cachedir_base.join("cache");

    let fs = new_overlay(cachedir_base.join("overlay"),
                         &cachedir,
                         &rootrepo_url,
                         branch.as_ref())
        .expect("Unable to create workspace");

    #[cfg(target_os = "linux")]
    {
        let uid = uid.unwrap_or_default();
        let gid = gid.unwrap_or_default();

        mount_fuse(mountpoint, fs, uid.get(), gid.get(), umask.get());
    }
}

fn log(range: CommitRange) {
    let start_commit = match range {
        CommitRange {
            start,
            end: None
        } => start,

        _ => unimplemented!("Only unbounded and ranges with only a start supported")
    };

    let log_command = commandstream::Command::Log { start_commit };
    commandstream::send_command(log_command);
}

fn main() {
    init_logging();

    let args: Arguments = Arguments::from_args();
    match args {
        Arguments::Mount(subargs) => mount(subargs),

        Arguments::Log { commit_range } => log(commit_range),

        Arguments::Status => commandstream::send_command(commandstream::Command::Status),

        Arguments::Branch { new_branch: Some(name) } => {
            let command = commandstream::Command::CreateBranch { name };

            commandstream::send_command(command)
        }

        Arguments::Branch { new_branch: None } | Arguments::Update => {
            let command = commandstream::Command::CurrentBranch {
                target_branch: None
            };

            commandstream::send_command(command)
        }

        Arguments::Switch { target_branch } => {
            let command = commandstream::Command::CurrentBranch {
                target_branch:Some(target_branch)
            };

            commandstream::send_command(command)
        }

        Arguments::Revert { revert_glob: path_glob, dry_run } => {
            let command = commandstream::Command::Revert {
                path_glob, dry_run
            };

            commandstream::send_command(command)
        }

        Arguments::Commit { message } =>
            commandstream::send_command(commandstream::Command::Commit { message })
    }
}

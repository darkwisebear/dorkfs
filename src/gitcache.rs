use std::{
    fs::File,
    io::{Write, Seek, SeekFrom},
    path::Path,
    fmt::{self, Debug, Formatter},
    result,
    sync::Mutex
};

use git2;
use tempfile;
use futures::future;
use failure::{self, Fail, format_err};

use crate::cache::*;

type GitResult<T> = result::Result<T, git2::Error>;
type GitGetFuture<T> = future::Ready<Result<T>>;

#[derive(Debug, Fail)]
enum GitLayerError {
    #[fail(display = "Git library error: {}", _0)]
    GitError(git2::Error),

    #[fail(display = "Unusable object type: {}", _0)]
    BadObjectType(git2::ObjectType),

    #[fail(display = "Unknown object type")]
    UnknownObjectType,

    #[fail(display = "Git operation failed")]
    RuntimeError(failure::Error)
}

impl LayerError for GitLayerError {}

impl From<git2::Error> for GitLayerError {
    fn from(err: git2::Error) -> Self {
        GitLayerError::GitError(err)
    }
}

struct GitCacheImpl {
    repo: git2::Repository
}

impl Debug for GitCacheImpl {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "GitCache")
    }
}

impl GitCacheImpl {
    #[cfg(test)]
    fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(GitCacheImpl {
            repo: git2::Repository::init(path).map_err(GitLayerError::from)?
        })
    }

    fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(GitCacheImpl {
            repo: git2::Repository::open(path).map_err(GitLayerError::from)?
        })
    }

    fn filemode_from_direntry(dir_entry: &DirectoryEntry) -> i32 {
        match dir_entry.object_type {
            ObjectType::File => 0o100644,
            ObjectType::Directory => 0o40000,
            ObjectType::Commit => 0o160000,
            ObjectType::Symlink => 0o120000
        }
    }

    fn cache_ref_to_oid(cache_ref: &CacheRef) -> git2::Oid {
        git2::Oid::from_bytes(&cache_ref.0[0..20]).unwrap()
    }

    fn oid_to_cache_ref(oid: git2::Oid) -> CacheRef {
        let mut result = CacheRef::null();
        let oid_subslice = &mut result.0[0..20];
        oid_subslice.copy_from_slice(oid.as_bytes());
        result
    }

    fn git_type_to_object_type(kind: Option<git2::ObjectType>) -> Result<ObjectType> {
        match kind {
            Some(git2::ObjectType::Blob) => Ok(ObjectType::File),
            Some(git2::ObjectType::Tree) => Ok(ObjectType::Directory),
            Some(git2::ObjectType::Commit) => Ok(ObjectType::Commit),
            Some(objtype) => Err(GitLayerError::BadObjectType(objtype).into()),
            None => Err(GitLayerError::UnknownObjectType.into())
        }
    }

    fn branch_to_ref<S: AsRef<str>>(branch: S) -> String {
        format!("refs/heads/{}", branch.as_ref())
    }

    fn do_commit(&self, commit: Commit) -> GitResult<git2::Oid> {
        let sig = git2::Signature::now("John Doe", "john.doe@example.com")?;
        let parents = commit.parents.into_iter().map(|parent|
            self.repo.find_commit(Self::cache_ref_to_oid(&parent)))
            .collect::<GitResult<Vec<_>>>()?;
        let parents_ref = parents.iter().collect::<Vec<_>>();
        let tree = self.repo.find_tree(Self::cache_ref_to_oid(&commit.tree))?;
        self.repo.commit(None, &sig, &sig,
                    commit.message.as_str(), &tree, parents_ref.as_slice())
    }

    fn tree_entry_to_dir_entry<'tree>(&self, tree_entry: &git2::TreeEntry<'tree>)
        -> Result<DirectoryEntry> {
        Ok(DirectoryEntry {
            name: String::from_utf8_lossy(tree_entry.name_bytes()).into_owned(),
            cache_ref: Self::oid_to_cache_ref(tree_entry.id()),
            object_type: Self::git_type_to_object_type(tree_entry.kind())?,
            size: self.repo.find_blob(tree_entry.id())
                .map(|blob| blob.content().len())
                .unwrap_or(0) as u64
        })
    }

    fn get_branch_oid<S: AsRef<str>>(&self, branch: S) -> GitResult<git2::Oid> {
        self.repo.refname_to_id(Self::branch_to_ref(branch).as_str())
    }

    #[cfg(test)]
    fn set_reference(&self, branch: &str, cache_ref: &CacheRef) -> Result<()> {
        self.repo.find_reference(Self::branch_to_ref(branch).as_str())
            .and_then(|mut reference|
                reference.set_target(Self::cache_ref_to_oid(cache_ref),
                                     format!("Set {} to {}", branch, cache_ref).as_str()))
            .map(|_| ())
            .map_err(|e| GitLayerError::from(e).into())
    }
}

impl CacheLayer for GitCacheImpl {
    type File = File;
    type Directory = Vec<DirectoryEntry>;
    type GetFuture = GitGetFuture<CacheObject<File, Vec<DirectoryEntry>>>;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>> {
        let gitobj =
            self.repo.find_object(Self::cache_ref_to_oid(cache_ref), None)
                .map_err(GitLayerError::from)?;
        match gitobj.kind() {
            Some(git2::ObjectType::Blob) => {
                let blob = gitobj.into_blob()
                    .expect("Unable to convert object into blob");
                let mut content_file = tempfile::tempfile()?;
                content_file.write_all(blob.content())?;
                content_file.seek(SeekFrom::Start(0))?;
                Ok(CacheObject::File(content_file))
            },
            Some(git2::ObjectType::Tree) => {
                let tree = gitobj.into_tree()
                    .expect("Unable to convert object into tree");
                let dir = tree.iter()
                    .map(|tree_entry|
                        self.tree_entry_to_dir_entry(&tree_entry))
                    .collect::<Result<Vec<DirectoryEntry>>>()?;
                Ok(CacheObject::Directory(dir))
            },
            Some(git2::ObjectType::Commit) => {
                let commit = gitobj.into_commit()
                    .expect("Unable to convert object into commit");
                let committed_date = {
                    use chrono::{TimeZone, FixedOffset};
                    let time = commit.time();
                    let offset = FixedOffset::east_opt(time.offset_minutes())
                        .ok_or(GitLayerError::RuntimeError(format_err!("Bad time offset").into()))?;
                    offset.timestamp(time.seconds(), 0)
                };
                let commit = Commit {
                    tree: Self::oid_to_cache_ref(commit.tree_id()),
                    parents: commit.parent_ids()
                        .map(Self::oid_to_cache_ref)
                        .collect(),
                    message: String::from_utf8_lossy(commit.message_bytes()).into_owned(),
                    committed_date
                };
                Ok(CacheObject::Commit(commit))
            },
            Some(objtype) => Err(GitLayerError::BadObjectType(objtype).into()),
            None => Err(GitLayerError::UnknownObjectType.into())
        }
    }

    fn add_file_by_path<P: AsRef<Path>>(&self, source_path: P) -> Result<CacheRef> {
        self.repo.blob_path(source_path.as_ref())
            .map(Self::oid_to_cache_ref)
            .map_err(|e| GitLayerError::GitError(e).into())
    }

    fn add_directory<I: IntoIterator<Item=DirectoryEntry>>(&self, items: I) -> Result<CacheRef> {
        self.repo.treebuilder(None)
            .and_then(|mut builder| {
                for entry in items {
                    let filemode = Self::filemode_from_direntry(&entry);
                    let entry_oid = Self::cache_ref_to_oid(&entry.cache_ref);
                    builder.insert(entry.name, entry_oid, filemode)?;
                }

                builder.write()
            })
            .map(Self::oid_to_cache_ref)
            .map_err(|e| GitLayerError::GitError(e).into())
    }

    fn add_commit(&self, commit: Commit) -> Result<CacheRef> {
        self.do_commit(commit)
            .map(Self::oid_to_cache_ref)
            .map_err(|git_err| GitLayerError::GitError(git_err).into())
    }

    fn get_head_commit<S: AsRef<str>>(&self, branch: S) -> Result<Option<CacheRef>> {
        match self.get_branch_oid(branch.as_ref()) {
            Ok(oid) => Ok(Some(Self::oid_to_cache_ref(oid))),
            Err(err) => match err.code() {
                git2::ErrorCode::NotFound => Ok(None),
                _ => Err(GitLayerError::GitError(err).into())
            }
        }
    }

    fn merge_commit<S: AsRef<str>>(&self, branch: S, cache_ref: &CacheRef) -> Result<CacheRef> {
        let reference = self.get_branch_oid(branch.as_ref());
        match reference {
            Ok(reference) => self.repo.find_commit(reference),
            Err(git_err) => match git_err.code() {
                git2::ErrorCode::NotFound => return self.create_branch(branch.as_ref(), cache_ref)
                        .map(|_| *cache_ref),
                _ => Err(git_err)
            }
        }
        .and_then(|commit1| {
            let commit2 = self.repo.find_commit(Self::cache_ref_to_oid(&cache_ref))?;
            let mut merge_opts = git2::MergeOptions::new();

            merge_opts.fail_on_conflict(true);
            let tree_id = self.repo.merge_commits(&commit1, &commit2,
                                                  Some(&merge_opts))
                .and_then(|mut index| {
                    self.repo.set_index(&mut index);
                    index.write_tree()
                })?;

            let sig = git2::Signature::now("John Doe", "john.doe@example.com")?;
            let commits = [&commit1, &commit2];
            let tree = self.repo.find_tree(tree_id)?;
            let commit_msg =
                format!("Merge {} into {} on branch {}", cache_ref, commit1.id(), branch.as_ref());
            self.repo.commit(Some(Self::branch_to_ref(branch).as_str()),
                             &sig, &sig, commit_msg.as_str(), &tree,
                             &commits[..])

        })
        .map(Self::oid_to_cache_ref)
        .map_err(|e| GitLayerError::GitError(e).into())
    }

    fn create_branch<S: AsRef<str>>(&self, branch: S, cache_ref: &CacheRef) -> Result<()> {
        self.repo.reference(Self::branch_to_ref(branch.as_ref()).as_str(),
                            Self::cache_ref_to_oid(cache_ref), false,
                            format!("Create reference {}", branch.as_ref()).as_str())
            .map_err(|e| GitLayerError::GitError(e).into())
            .map(|_| ())
    }

    fn get_poll(&self, cache_ref: &CacheRef) -> Self::GetFuture {
        future::ready(self.get(cache_ref))
    }
}

#[derive(Debug)]
pub struct GitCache {
    inner: Mutex<GitCacheImpl>
}

impl From<GitCacheImpl> for GitCache {
    fn from(cache: GitCacheImpl) -> Self {
        Self {
            inner: Mutex::new(cache)
        }
    }
}

impl GitCache {
    #[cfg(test)]
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        GitCacheImpl::new(path).map(Self::from)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        GitCacheImpl::open(path).map(Self::from)
    }

    #[cfg(test)]
    pub fn set_reference(&self, branch: &str, cache_ref: &CacheRef) -> Result<()> {
        self.inner.lock().unwrap().set_reference(branch, cache_ref)
    }
}

impl CacheLayer for GitCache {
    type File = File;
    type Directory = Vec<DirectoryEntry>;
    type GetFuture = GitGetFuture<CacheObject<File, Vec<DirectoryEntry>>>;

    fn get(&self, cache_ref: &CacheRef) -> Result<CacheObject<Self::File, Self::Directory>> {
        self.inner.lock().unwrap().get(cache_ref)
    }

    fn add_file_by_path<P: AsRef<Path>>(&self, source_path: P) -> Result<CacheRef> {
        self.inner.lock().unwrap().add_file_by_path(source_path)
    }

    fn add_directory<I: IntoIterator<Item=DirectoryEntry>>(&self, items: I) -> Result<CacheRef> {
        self.inner.lock().unwrap().add_directory(items)
    }

    fn add_commit(&self, commit: Commit) -> Result<CacheRef> {
        self.inner.lock().unwrap().add_commit(commit)
    }

    fn get_head_commit<S: AsRef<str>>(&self, branch: S) -> Result<Option<CacheRef>> {
        self.inner.lock().unwrap().get_head_commit(branch)
    }

    fn merge_commit<S: AsRef<str>>(&self, branch: S, cache_ref: &CacheRef) -> Result<CacheRef> {
        self.inner.lock().unwrap().merge_commit(branch, cache_ref)
    }

    fn create_branch<S: AsRef<str>>(&self, branch: S, cache_ref: &CacheRef) -> Result<()> {
        self.inner.lock().unwrap().create_branch(branch, cache_ref)
    }

    fn get_poll(&self, cache_ref: &CacheRef) -> Self::GetFuture {
        self.inner.lock().unwrap().get_poll(cache_ref)
    }
}

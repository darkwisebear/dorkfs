use std::io::Cursor;
use std::fmt::{self, Formatter, Debug};
use std::sync::Arc;
use std::mem::replace;
use std::vec;
use std::iter::FromIterator;

use failure::{Fallible, Error};
use serde::{Deserialize, Deserializer, de::Visitor};
use chrono::{DateTime, FixedOffset};

use crate::{
    cache::{CacheObject, Commit, CacheRef, DirectoryEntry, ObjectType}
};

use super::{GithubBlob, GithubTree};

#[derive(Debug, Clone, Deserialize)]
pub struct Author {
    pub name: String,
    pub email: String
}

#[derive(Debug, Clone, Deserialize)]
pub struct GitObjectId {
    pub oid: String
}

impl GitObjectId {
    pub fn try_into_cache_ref(self) -> Result<CacheRef, Error> {
        super::sha_to_cache_ref(self.oid)
    }
}

impl From<CacheRef> for GitObjectId {
    fn from(cache_ref: CacheRef) -> Self {
        Self { oid: super::cache_ref_to_sha(cache_ref.to_string()) }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitConnection {
    pub nodes: Vec<GitObjectId>,
    pub page_info: PageInfo
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageInfo {
    pub has_next_page: bool
}

const FILE_MODE_SYMLINK: u32 = 0o120_000;
const FILE_MODE_EXECUTABLE: u32 = 0o100_755;
const FILE_MODE_BLOB: u32 = 0o100_644;
const FILE_MODE_DIRECTORY: u32 = 0o040_000;
const FILE_MODE_COMMIT: u32 = 0o160_000;

#[derive(Debug, Clone, Deserialize)]
pub struct TreeEntry {
    pub name: String,
    pub mode: u32,
    #[serde(flatten)]
    pub oid: GitObjectId,
    #[serde(default)]
    pub object: Option<GitObject>
}

impl TreeEntry {
    pub fn get_entry_type(&self) -> Fallible<ObjectType> {
        match self.mode {
            FILE_MODE_EXECUTABLE |
            FILE_MODE_BLOB => Ok(ObjectType::File),
            FILE_MODE_DIRECTORY |
            FILE_MODE_COMMIT => Ok(ObjectType::Directory),
            FILE_MODE_SYMLINK => Ok(ObjectType::Symlink),
            _ => bail!("Unknown file mode in GitHub directory entry")
        }
    }

    pub fn get_entry_size(&self) -> u64 {
        if let Some(GitObject::Blob { byte_size, .. }) = self.object {
            byte_size as u64
        } else {
            0u64
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeList<T> where T: Debug+Clone {
    nodes: Vec<T>
}

impl<T: Debug+Clone> Default for NodeList<T> {
    fn default() -> Self {
        Self {
            nodes: Vec::new()
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitCommit {
    author: Option<Author>,
    tree: Option<GitObjectId>,
    message: Option<String>,
    parents: Option<CommitConnection>,
    #[serde(flatten)]
    oid: Option<GitObjectId>,
    committed_date: Option<DateTime<FixedOffset>>
}

impl Into<Fallible<Commit>> for GitCommit {
    fn into(self) -> Fallible<Commit> {
        if let GitCommit {
            tree: Some(tree),
            parents: Some(parents),
            message: Some(message),
            committed_date: Some(committed_date),
            author: _,
            oid: _
        } = self {
            let cache_ref = tree.try_into_cache_ref()?;
            let converted_parents =
                parents.nodes.into_iter()
                    .map(|parent|
                        parent.try_into_cache_ref());
            let commit = Commit {
                tree: cache_ref,
                parents: converted_parents.collect::<Fallible<Vec<CacheRef>>>()?,
                message,
                committed_date
            };

            Ok(commit)
        } else {
            unimplemented!("TODO: Cope with incomplete commit data")
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "__typename")]
pub enum GitObject {
    Commit {
        #[serde(flatten)]
        oid: Option<GitObjectId>,
        #[serde(default)]
        history: NodeList<GitCommit>
    },

    #[serde(rename_all = "camelCase")]
    Blob {
        #[serde(default, deserialize_with = "arc_u8_from_string")]
        text: Option<Arc<[u8]>>,
        is_truncated: bool,
        byte_size: usize,
        #[serde(flatten)]
        oid: Option<GitObjectId>
    },

    Tree {
        #[serde(flatten)]
        oid: Option<GitObjectId>,
        #[serde(default)]
        entries: Option<Vec<TreeEntry>>
    }
}

pub fn arc_u8_from_string<'de, D>(deserializer: D) -> Result<Option<Arc<[u8]>>, D::Error>
    where D: Deserializer<'de> {
    struct StringToVecVisitor;

    impl<'de> Visitor<'de> for StringToVecVisitor {
        type Value = Option<Arc<[u8]>>;

        fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
            write!(formatter, "Just. A. String.")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
            Ok(Some(v.as_bytes().into()))
        }

        fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
            Ok(Some(v.into_bytes().into()))
        }
    }

    deserializer.deserialize_string(StringToVecVisitor)
}

pub enum GitObjIter {
    Err(failure::Error),
    Blob(CacheRef, GithubBlob),
    TreeEntries(CacheRef, vec::IntoIter<TreeEntry>),
    CommitHistory(vec::IntoIter<GitCommit>),
    Finished
}

impl Iterator for GitObjIter {
    type Item = Fallible<(CacheRef, CacheObject<GithubBlob, GithubTree>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let cur_state = replace(self, GitObjIter::Finished);
        match cur_state {
            GitObjIter::Blob(cache_ref, file) => Some(Ok((cache_ref, CacheObject::File(file)))),

            GitObjIter::TreeEntries(cache_ref, entries) => {
                let dir: Fallible<Vec<DirectoryEntry>> = entries.map(|entry| {
                    let object_type = entry.get_entry_type()?;
                    let size = entry.get_entry_size();

                    let cache_ref = entry.oid.try_into_cache_ref()?;

                    let dir_entry = DirectoryEntry {
                        object_type,
                        size,
                        name: entry.name,
                        cache_ref
                    };

                    Ok(dir_entry)
                }).collect();

                match dir {
                    Ok(dir) => Some(Ok((cache_ref, CacheObject::Directory(GithubTree::from_iter(dir))))),
                    Err(e) => Some(Err(e))
                }
            }

            GitObjIter::CommitHistory(mut history) => {
                let commit = history.next().map(|mut commit| {
                    let oid = commit.oid.take();
                    let cache_ref = oid.ok_or_else(|| format_err!("Missing Oid"))
                        .and_then(|oid| oid.try_into_cache_ref())?;
                    let commit: Fallible<Commit> = commit.into();
                    commit.map(move |commit|
                        (cache_ref, CacheObject::Commit(commit)))
                });

                if commit.is_some() {
                    *self = GitObjIter::CommitHistory(history);
                }

                commit
            }

            GitObjIter::Err(e) => Some(Err(e)),

            GitObjIter::Finished => None
        }
    }
}

impl IntoIterator for GitObject {
    type Item = Fallible<(CacheRef, CacheObject<GithubBlob, GithubTree>)>;
    type IntoIter = GitObjIter;

    fn into_iter(self) -> Self::IntoIter {
        let oid = self.get_oid();

        let cache_ref = oid.ok_or_else(||
            format_err!("Missing oid in object"))
            .and_then(|oid| oid.clone().try_into_cache_ref());

        match self {
            GitObject::Blob { text, .. } => {
                let cache_ref = match cache_ref {
                    Ok(cache_ref) => cache_ref,
                    Err(e) => return GitObjIter::Err(e)
                };

                let data = match text.ok_or_else(||
                    format_err!("Attempt to convert a blob without data into a cache object")) {
                    Ok(content) => Cursor::new(content),
                    Err(e) => return GitObjIter::Err(e)
                };

                let blob = GithubBlob {
                    data
                };
                GitObjIter::Blob(cache_ref, blob)
            }

            GitObject::Commit { history, oid: _ } =>
                GitObjIter::CommitHistory(history.nodes.into_iter()),

            GitObject::Tree { entries, oid: _ } => {
                let cache_ref = match cache_ref {
                    Ok(cache_ref) => cache_ref,
                    Err(e) => return GitObjIter::Err(e)
                };

                GitObjIter::TreeEntries(cache_ref, entries.unwrap_or_default().into_iter())
            }
        }
    }
}

impl GitObject {
    pub fn get_oid(&self) -> Option<&GitObjectId> {
        match *self {
            GitObject::Blob { ref oid, .. } => oid.as_ref(),
            GitObject::Tree { ref oid, .. } => oid.as_ref(),
            GitObject::Commit { ref oid, .. } => oid.as_ref()
        }
    }

    pub fn set_oid(&mut self, new_oid: GitObjectId) {
        let oid = match *self {
            GitObject::Blob { ref mut oid, .. } => oid,
            GitObject::Tree { ref mut oid, .. } => oid,
            GitObject::Commit { ref mut oid, .. } => oid
        };

        *oid = Some(new_oid)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ref {
    pub name: String,
    pub target: GitObject
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Repository {
    pub object: Option<GitObject>,
    pub default_branch_ref: Option<Ref>,
    #[serde(rename = "ref")]
    pub branch_ref: Option<Ref>
}

#[derive(Debug, Clone, Deserialize)]
pub struct Query {
    pub repository: Repository
}

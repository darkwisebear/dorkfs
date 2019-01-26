use crate::cache::{DirectoryEntry, ObjectType, Commit};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitBlob {
    pub content: String,
    pub encoding: String,
    #[serde(skip_serializing_if="Option::is_none")]
    pub size: Option<usize>
}

#[derive(Debug, Clone, Deserialize)]
pub struct GitObjectReference {
    pub url: String,
    pub sha: String
}

#[derive(Debug, Clone, Serialize)]
pub struct GitTreeEntry {
    pub path: String,
    pub mode: String,
    #[serde(rename = "type")]
    pub obj_type: String,
    pub sha: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub content: Option<String>
}

impl From<DirectoryEntry> for GitTreeEntry {
    fn from(dir_entry: DirectoryEntry) -> Self {
        let sha = super::cache_ref_to_sha(dir_entry.cache_ref.to_string());
        let (obj_type, mode) = match dir_entry.object_type {
            ObjectType::File =>      ("blob",   "100644"),
            ObjectType::Directory => ("tree",   "040000"),
            ObjectType::Commit =>    ("commit", "160000"),
            ObjectType::Symlink =>   ("blob",   "120000")
        };

        Self {
            path: dir_entry.name,
            mode: mode.to_string(),
            obj_type: obj_type.to_string(),
            sha: Some(sha),
            content: None
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct GitTree {
    pub tree: Vec<GitTreeEntry>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub base_tree: Option<String>
}

#[derive(Debug, Clone, Serialize)]
pub struct GitCommit {
    pub message: String,
    pub tree: String,
    pub parents: Vec<String>
}

impl From<Commit> for GitCommit {
    fn from(commit: Commit) -> Self {
        let parents = commit.parents.into_iter()
            .map(|cache_ref| super::cache_ref_to_sha(
                cache_ref.to_string())).collect();
        Self {
            message: commit.message,
            tree: super::cache_ref_to_sha(commit.tree.to_string()),
            parents
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct GitMergeResponse {
    pub sha: String
}

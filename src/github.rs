use std::path::Path;
use std::io::{self, Read, Seek, SeekFrom, Cursor};
use std::vec;
use std::fs;
use std::sync::{Arc, Weak, Mutex, mpsc::channel};
use std::str::{self, FromStr};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::default::Default;

use http::{uri::InvalidUri, request};
use hyper::{self, Uri, Body, Request, Response, Client, StatusCode, client:: HttpConnector};
use hyper_tls::{self, HttpsConnector};
use futures::{Stream, Future, future};
use tokio::{prelude::FutureExt, runtime::Runtime, self};
use serde_json::de::from_slice as from_json_slice;
use serde_json::ser::to_string as to_json_string;
use failure;
use base64;
use bytes::Bytes;

use crate::cache::{self, DirectoryEntry, ReadonlyFile, CacheObject, CacheError, CacheRef,
                   CacheLayer, Directory, LayerError, Commit};

lazy_static! {
    static ref TOKIO_RUNTIME: Mutex<Weak<Runtime>> = Mutex::new(Weak::new());
}

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "Invalid GitHub URI {}", _0)]
    InvalidUri(InvalidUri),
    #[fail(display = "Unexpected GraphQL response from GitHub: {}", _0)]
    UnexpectedGraphQlResponse(&'static str),
    #[fail(display = "TLS connection error {}", _0)]
    TlsConnectionError(hyper_tls::Error),
    #[fail(display = "async IO error {}", _0)]
    TokioIoError(tokio::io::Error),
    #[fail(display = "Unable to convert to UTF-8 {}", _0)]
    Utf8Error(str::Utf8Error),
    #[fail(display = "Hyper error {}", _0)]
    HyperError(hyper::Error),
    #[fail(display = "Only Base64 encoding is supported for files")]
    UnsupportedBlobEncoding,
    #[fail(display = "Unable to fetch reference {}", _0)]
    UnknownReference(String),
    #[fail(display = "Operation timed out")]
    TimeoutError,
    #[fail(display = "Git operation failed: {}", _0)]
    GitOperationFailure(StatusCode)
}

impl LayerError for Error {}

impl From<hyper_tls::Error> for Error {
    fn from(err: hyper_tls::Error) -> Self {
        Error::TlsConnectionError(err)
    }
}

impl From<InvalidUri> for Error {
    fn from(err: InvalidUri) -> Self {
        Error::InvalidUri(err)
    }
}

impl From<tokio::io::Error> for Error {
    fn from(err: tokio::io::Error) -> Self {
        Error::TokioIoError(err)
    }
}

impl From<str::Utf8Error> for Error {
    fn from(err: str::Utf8Error) -> Self {
        Error::Utf8Error(err)
    }
}

impl From<hyper::Error> for Error {
    fn from(err: hyper::Error) -> Self {
        Error::HyperError(err)
    }
}

pub fn sha_to_cache_ref(mut sha: String) -> Result<CacheRef, failure::Error> {
    sha.push_str("000000000000000000000000");
    CacheRef::from_str(sha.as_str())
}

pub fn cache_ref_to_sha(mut cache_ref_str: String) -> String {
    cache_ref_str.truncate(40);
    cache_ref_str
}

#[derive(Debug)]
pub struct GithubBlob {
    data: Cursor<Arc<[u8]>>
}

impl Read for GithubBlob {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.data.read(buf)
    }
}

impl Seek for GithubBlob {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.data.seek(pos)
    }
}

impl ReadonlyFile for GithubBlob {}

#[derive(Debug, Clone)]
pub struct GithubTree(Vec<DirectoryEntry>);

impl Directory for GithubTree {}

impl IntoIterator for GithubTree {
    type Item = DirectoryEntry;
    type IntoIter = vec::IntoIter<DirectoryEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Debug)]
pub struct Github {
    graphql_uri: Uri,
    rest_base_uri: String,
    org: String,
    repo: String,
    token: String,
    tokio: Arc<Runtime>,
    http_client: Client<HttpsConnector<HttpConnector>>,
    query_cache: Mutex<HashMap<CacheRef, graphql::GitObject>>
}

mod restv3 {
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
}

mod graphql {
    use std::io::Cursor;
    use std::fmt::{self, Formatter};
    use std::sync::Arc;

    use failure::Error;
    use serde::{Deserializer, de::Visitor};
    use chrono::{DateTime, Local};

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

    const FILE_MODE_SYMLINK: u32 = 0o120000;

    #[derive(Debug, Clone, Deserialize)]
    pub struct TreeEntry {
        pub name: String,
        pub mode: u32,
        #[serde(flatten)]
        pub oid: GitObjectId,
        pub object: GitObject
    }

    impl TreeEntry {
        pub fn get_entry_type(&self) -> ObjectType {
            match self.object {
                GitObject::Commit { .. } => ObjectType::Commit,
                GitObject::Tree { .. } => ObjectType::Directory,
                GitObject::Blob { .. } => if self.mode == FILE_MODE_SYMLINK {
                    ObjectType::Symlink
                } else {
                    ObjectType::File
                }
            }
        }

        pub fn get_entry_size(&self) -> u64 {
            if let GitObject::Blob { byte_size, .. } = self.object {
                byte_size as u64
            } else {
                0u64
            }
        }
    }

    #[derive(Debug, Clone, Deserialize)]
    #[serde(tag = "__typename")]
    pub enum GitObject {
        #[serde(rename_all = "camelCase")]
        Commit {
            author: Option<Author>,
            tree: Option<GitObjectId>,
            message: Option<String>,
            parents: Option<CommitConnection>,
            #[serde(flatten)]
            oid: Option<GitObjectId>,
            committed_date: Option<DateTime<Local>>
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
            #[serde(default)]
            entries: Option<Vec<TreeEntry>>
        }
    }

    fn arc_u8_from_string<'de, D>(deserializer: D) -> Result<Option<Arc<[u8]>>, D::Error>
        where D: Deserializer<'de> {
        struct StringToVecVisitor;

        impl<'de> Visitor<'de> for StringToVecVisitor {
            type Value = Option<Arc<[u8]>>;

            fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
                write!(formatter, "Just. A. String.")
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(Some(v.into_bytes().into()))
            }
        }

        deserializer.deserialize_string(StringToVecVisitor)
    }

    impl Into<Result<CacheObject<GithubBlob, GithubTree>, Error>> for GitObject {
        fn into(self) -> Result<CacheObject<GithubBlob, GithubTree>, Error> {
            match self {
                GitObject::Commit {
                    tree: Some(tree),
                    parents: Some(parents),
                    message: Some(message),
                    committed_date: Some(committed_date),
                    author: _,
                    oid: _
                } => {
                    let cache_ref = tree.try_into_cache_ref()?;
                    let converted_parents =
                        parents.nodes.into_iter()
                            .map(|parent|
                                parent.try_into_cache_ref());
                    let commit = Commit {
                        tree: cache_ref,
                        parents: converted_parents.collect::<Result<Vec<CacheRef>, Error>>()?,
                        message,
                        committed_date
                    };

                    Ok(CacheObject::Commit(commit))
                }

                GitObject::Commit { .. } => {
                    unimplemented!("TODO: Cope with incomplete commit data")
                }

                GitObject::Blob { text, .. } => {
                    let content = text.expect("Attempt to convert a blob without \
                    data into a cache object");
                    let blob = GithubBlob {
                        data: Cursor::new(content)
                    };
                    Ok(CacheObject::File(blob))
                }

                GitObject::Tree { entries } => {
                    let dir
                        = entries.unwrap_or_default().into_iter().map(
                            |entry| -> Result<DirectoryEntry, Error> {
                                Ok(DirectoryEntry {
                                    object_type: entry.get_entry_type(),
                                    size: entry.get_entry_size(),
                                    name: entry.name,
                                    cache_ref: entry.oid.try_into_cache_ref()?,
                                })
                            });
                    let dir_vec = dir.collect::<Result<_, Error>>()?;
                    Ok(CacheObject::Directory(GithubTree(dir_vec)))
                }
            }
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

}

#[derive(Debug, Clone, Deserialize)]
struct GraphQLQueryResponse {
    data: graphql::Query
}

impl CacheLayer for Github {
    type File = GithubBlob;
    type Directory = GithubTree;

    fn get(&self, cache_ref: &CacheRef)
           -> cache::Result<CacheObject<Self::File, Self::Directory>> {
        let object = self.query_object(cache_ref, true)?;
        let cache_obj: Result<_, _> = object.into();
        Ok(cache_obj.unwrap())
    }

    fn add_file_by_path(&self, source_path: &Path) -> cache::Result<CacheRef> {
        let source = fs::File::open(source_path)?;
        self.post_blob_data(source)
    }

    fn add_directory(&self, items: &mut Iterator<Item=DirectoryEntry>) -> cache::Result<CacheRef> {
        let git_tree_entries = items.map(restv3::GitTreeEntry::from);
        let git_tree = restv3::GitTree {
            tree: git_tree_entries.collect(),
            base_tree: None
        };
        let git_tree_json = to_json_string(&git_tree)?;
        debug!("Adding directory with request {}", git_tree_json.as_str());
        self.post_git_object_json(git_tree_json, "trees")
    }

    fn add_commit(&self, commit: Commit) -> cache::Result<CacheRef> {
        to_json_string(&restv3::GitCommit::from(commit))
            .map_err(Into::into)
            .and_then(|git_commit_json|
                self.post_git_object_json(git_commit_json, "commits"))
    }

    fn get_head_commit(&self, branch: &str) -> cache::Result<Option<CacheRef>> {
        self.get_ref_info(Some(branch))
            .map(|ref_info_result|
                ref_info_result.map(|(_, cache_ref)| cache_ref))
    }

    fn merge_commit(&mut self, branch: &str, cache_ref: CacheRef) -> cache::Result<CacheRef> {
        self.update_branch_head(branch, cache_ref)
    }

    fn create_branch(&mut self, branch: &str, cache_ref: CacheRef) -> Result<(), CacheError> {
        self.create_git_ref(branch, cache_ref)
    }
}

impl Github {
    pub fn new(base_url: &str, org: &str, repo: &str, token: &str)
               -> Result<Self, Error> {
        let tokio = {
            let mut runtime = TOKIO_RUNTIME.lock().unwrap();
            match runtime.upgrade() {
                Some(tokio) => tokio,
                None => {
                    let t = Arc::new(Runtime::new()?);
                    *runtime = Arc::downgrade(&t);
                    t
                }
            }
        };

        let http_client =
            Client::builder().build(HttpsConnector::new(4)?);

        // Here we will also parse the given str. Therefore we can unwrap the parses later as we
        // already verified that the URL is fine.
        let uri: Uri = {
            if base_url.ends_with('/') {
                (&base_url[0..base_url.len() - 1]).parse()?
            } else {
                base_url.parse()?
            }
        };

        let github_host = uri.host().expect("No host name given");
        let (graphql_uri, rest_base_uri) = if github_host.ends_with("github.com") {
            ("https://api.github.com/graphql".parse().unwrap(),
             "https://api.github.com".to_string())
        } else {
            (format!("{}/graphql", uri).parse().unwrap(), format!("{}/v3", uri))
        };

        Ok(Github {
            graphql_uri,
            rest_base_uri,
            org: org.to_string(),
            repo: repo.to_string(),
            token: token.to_string(),
            tokio,
            http_client,
            query_cache: Default::default()
        })
    }

    pub fn get_default_branch(&self) -> cache::Result<String> {
        self.get_ref_info(None)
            .and_then(|ref_info| ref_info
                .ok_or(Error::UnknownReference("(default branch)".to_string()).into())
                .map(|(ref_name,_)| ref_name))
    }

    pub fn get_blob_data(&self, cache_ref: &CacheRef) -> cache::Result<Vec<u8>> {
        let path = format!("{base}/repos/{org}/{repo}/git/blobs/{sha}",
                           base = self.rest_base_uri,
                           org = self.org,
                           repo = self.repo,
                           sha = &cache_ref.to_string()[0..40]);
        let uri = Uri::from_shared(path.into()).unwrap(); // This shouldn't fail as we already
        // checked the validity URL during
        // construction

        info!("Retrieving git blob from {}", uri);

        let mut request_builder = Request::get(uri);
        request_builder.header("Accept", "application/vnd.github.v3+json");
        let response =
            self.issue_request(request_builder, Body::empty())?;

        // TODO: Explicitly handle the case when an object can't be found.
        // In case the object wasn't found (or isn't a blob) this call will fail with a weird
        // error. This should be handled explicitly.
        let git_blob: restv3::GitBlob = from_json_slice(response.body().as_ref())?;
        if git_blob.encoding.as_str() == "base64" {
            base64::decode_config(git_blob.content.as_str(), base64::MIME)
                .map_err(|e|
                    CacheError::Custom("Base64 decodong failed", e.into()))
        } else {
            Err(Error::UnsupportedBlobEncoding.into())
        }
    }

    fn post_blob_data<R: Read>(&self, mut reader: R) -> cache::Result<CacheRef> {
        let mut input = Vec::new();
        reader.read_to_end(&mut input)?;

        let content = base64::encode_config(input.as_slice(), base64::MIME);
        let blob = restv3::GitBlob {
            content,
            encoding: "base64".to_string(),
            size: None
        };

        let obj_json = to_json_string(&blob)?;
        self.post_git_object_json(obj_json, "blobs")
    }

    fn post_git_object_json(&self, obj_json: String, git_obj_type: &str) -> cache::Result<CacheRef> {
        let path = format!("{base}/repos/{org}/{repo}/git/{obj_type}",
                           base = self.rest_base_uri,
                           org = self.org,
                           repo = self.repo,
                           obj_type = git_obj_type);
        // This shouldn't fail as we already checked the validity URL during construction
        let uri = Uri::from_shared(path.into()).unwrap();

        let mut request_builder = Request::post(uri);
        request_builder.header("Accept", "application/vnd.github.v3+json");
        let response =
            self.issue_request(request_builder, obj_json.into())?;
        let git_blob_created: restv3::GitObjectReference = from_json_slice(response.body().as_ref())?;

        sha_to_cache_ref(git_blob_created.sha)
            .map_err(|e|
                CacheError::Custom("Cannot decode SHA reploy from GitHub after blob creation",
                                   e))
    }

    fn fetch_remote_object(&self, cache_ref: &CacheRef, get_blob_contents: bool)
                           -> cache::Result<GraphQLQueryResponse> {
        let query = format!("{{\"query\":\" \
query {{ \
  repository(owner:\\\"{org}\\\",name:\\\"{repo}\\\") {{ \
    object(oid:\\\"{oid}\\\") {{ \
      __typename \
      ...on Blob {{ \
        {get_contents} \
        byteSize \
        isTruncated \
      }} \
      ...on Commit {{ \
        author {{ \
          name \
          email \
        }} \
        tree {{ \
          oid \
        }} \
        message \
        committedDate \
        parents(first:5) {{ \
          nodes {{ \
            oid \
          }} \
          pageInfo {{ \
            hasNextPage \
          }} \
        }} \
      }} \
      ...on Tree {{ \
        entries {{ \
          name \
          mode \
          oid \
          object {{ \
            __typename \
            ...on Blob {{ \
              isTruncated \
              byteSize \
            }} \
          }} \
        }} \
      }} \
    }} \
  }} \
}}\" \
}}", org = self.org, repo = self.repo, oid = &cache_ref.to_string()[0..40], get_contents = if get_blob_contents { "text" } else { "" });

        let response = self.execute_graphql_query(query)?;

        let mut get_response: GraphQLQueryResponse = from_json_slice(response.body().as_ref())?;
        debug!("Parsed response: {:?}", get_response);

        // If we've received a blob, set its oid since we may need it to get the object's
        // contents later
        if let Some(graphql::GitObject::Blob { ref mut oid, .. }) = get_response.data.repository.object {
            *oid = Some(graphql::GitObjectId::from(*cache_ref));
        }

        Ok(get_response)
    }

    fn query_object(&self, cache_ref: &CacheRef, get_blob_contents: bool)
                    -> cache::Result<graphql::GitObject> {
        let mut cache =
            self.query_cache.lock().unwrap();
        let obj = {
            let obj = match cache.entry(*cache_ref) {
                Entry::Occupied(occupied) => occupied.into_mut(),

                Entry::Vacant(vacant) => {
                    let fetched_obj =
                        self.fetch_remote_object(cache_ref, get_blob_contents)?;
                    let obj = fetched_obj.data.repository.object
                        .ok_or(Error::UnexpectedGraphQlResponse("Missing object"))?;
                    vacant.insert(obj)
                }
            };

            if get_blob_contents {
                self.ensure_git_object_data(cache_ref, obj)?;
            }

            obj.clone()
        };

        // If we've received a tree we extract the entries so that we don't have to query them
        // again if we need their metadata
        if let graphql::GitObject::Tree { entries: Some(ref entries) } = obj {
            for tree_entry in entries {
                let cache_ref =
                    tree_entry.oid.clone().try_into_cache_ref()
                        .map_err(|e| CacheError::Custom(
                            "Unable to convert the oid to a CacheRef", e))?;
                if let Entry::Vacant(cache_entry) = cache.entry(cache_ref) {
                    cache_entry.insert(tree_entry.object.clone());
                }
            }
        }

        Ok(obj)
    }

    fn ensure_git_object_data(&self, cache_ref: &CacheRef, object: &mut graphql::GitObject)
                              -> cache::Result<()> {
        match object {
            graphql::GitObject::Blob {
                ref mut text,
                ref mut is_truncated, ..
            } => {
                if text.is_none() || *is_truncated {
                    *text = Some(self.get_blob_data(cache_ref)?.into());
                    *is_truncated = false;
                }
            }

            // If the cached tree is missing its entries we add them directly to the cached
            // tree object
            graphql::GitObject::Tree { entries: None } => {
                let fetched_obj =
                    self.fetch_remote_object(cache_ref, false)?;
                *object = fetched_obj.data.repository.object
                    .ok_or(Error::UnexpectedGraphQlResponse("Missing object"))?;

                // Check if we received what we expected
                match object {
                    graphql::GitObject::Tree { entries: Some(_) } => (), // We're cool
                    graphql::GitObject::Tree { entries: None } =>
                        return Err(Error::UnexpectedGraphQlResponse(
                            "No entries received in tree").into()),
                    _ => {
                        error!("Unexpected object {:?}", *object);
                        return Err(Error::UnexpectedGraphQlResponse(
                            "Unexpectedly not receiving a tree object").into())
                    }
                }
            }

            _ => ()
        }

        Ok(())
    }

    fn execute_graphql_query(&self, query: String) -> cache::Result<Response<Bytes>> {
        debug!("Sending query to GitHub at {}: {}", &self.graphql_uri, query);

        let request_builder = Request::post(self.graphql_uri.clone());
        self.issue_request(request_builder, Body::from(query))
    }

    fn build_request(&self, mut request_builder: request::Builder, body: Body)
        -> impl Future<Item=Response<Bytes>, Error=Error> {
        let user_agent = format!("dorkfs/{}",
                                 option_env!("CARGO_PKG_VERSION").unwrap_or("(unknown)"));
        let request = request_builder
            .header("Authorization", format!("Bearer {}", &self.token).as_str())
            .header("User-Agent", user_agent)
            .body(body).unwrap();

        self.http_client.request(request)
            .map_err(Error::HyperError)
            .and_then(|response| {
                let (parts, body) = response.into_parts();
                let body =
                    body.map_err(Error::HyperError).concat2();
                (future::ok(parts), body)
            })
            .then(|r|
                r.map(|(parts, body)|
                    Response::from_parts(parts, body.into_bytes())))
    }

    fn execute_request<F, S>(&self, request_future: F) -> cache::Result<Response<S>>
        where F: Future<Item=Response<S>, Error=Error>+Send+'static,
              S: Send+'static {
        let (send, recv) = channel();

        let finalizing_future = request_future
            .timeout(::std::time::Duration::from_secs(60))
            .then(move |r|
                send.send(r.map_err(|e|
                    e.into_inner().unwrap_or(Error::TimeoutError)))
                    .map_err(|_|
                        unreachable!("Unexpected send error during GitHub request")));

        self.tokio.executor().spawn(finalizing_future);
        recv.recv().expect("Unexpected receive error during GitHub request")
            .map_err(Into::into)
    }

    fn issue_request(&self, request_builder: request::Builder, body: Body)
        -> cache::Result<Response<Bytes>> {
        let request_future =
            self.build_request(request_builder, body);
        let response = self.execute_request(request_future);
        for response in response.iter() {
            debug!("Raw response: {}", String::from_utf8_lossy(response.body().as_ref()));
        }
        response
    }

    fn get_ref_info(&self, branch: Option<&str>) -> Result<Option<(String, CacheRef)>, CacheError> {
        let ref_query = if let Some(branch_name) = branch {
            format!("ref(qualifiedName: \\\"refs/heads/{}\\\")", branch_name)
        } else {
            "defaultBranchRef".to_string()
        };

        let query = format!("{{\"query\":\" \
query {{ \
  repository(owner:\\\"{org}\\\",name:\\\"{repo}\\\") {{ \
    {ref_query} {{ \
      name \
      target {{ \
        __typename \
        oid \
      }} \
    }} \
  }} \
}} \
\"}}", ref_query = ref_query, org = self.org, repo = self.repo);
        let response = self.execute_graphql_query(query)
            .and_then(|response| {
                if !response.status().is_success() {
                    Err(Error::UnknownReference(
                        branch.unwrap_or("(default)").to_string()).into())
                } else {
                    Ok(response.into_body())
                }
            })?;
        let head_commit_response: GraphQLQueryResponse = from_json_slice(response.as_ref())?;

        let branch_ref = if branch.is_some() {
            head_commit_response.data.repository.branch_ref
        } else {
            head_commit_response.data.repository.default_branch_ref
        };

        match branch_ref {
            Some(graphql::Ref {
                     target: graphql::GitObject::Commit { oid: Some(oid), .. },
                     name
                 }
            ) => {
                oid.try_into_cache_ref()
                    .map(|cache_ref| Some((name, cache_ref)))
                    .map_err(|e| CacheError::Custom("Unparsable OID string", e))
            }

            Some(graphql::Ref { target: graphql::GitObject::Tree { .. }, .. }) => {
                Err(CacheError::UnexpectedObjectType(cache::ObjectType::Directory))
            }

            Some(graphql::Ref { target: graphql::GitObject::Blob { .. }, .. }) => {
                Err(CacheError::UnexpectedObjectType(cache::ObjectType::File))
            }

            Some(_) => {
                Err(CacheError::LayerError(
                    Error::UnexpectedGraphQlResponse("No branch ref received").into()))
            }

            None => Ok(None)
        }
    }

    fn update_branch_head(&self, branch: &str, cache_ref: CacheRef) -> cache::Result<CacheRef> {
        if let Some((ref_name, _)) = self.get_ref_info(Some(branch))? {
            // Ref exists, we need to issue a PATCH or a merge in case no fast-forward is
            // possible.
            let sha = cache_ref_to_sha(cache_ref.to_string());
            let base_uri = format!("{baseuri}/repos/{owner}/{repo}",
                                   baseuri=self.rest_base_uri,
                                   owner=self.org,
                                   repo=self.repo);
            let update_uri = format!("{baseuri}/git/refs/heads/{gitref}",
                                     baseuri=base_uri,
                                     gitref=ref_name);
            let update_body = format!(r#"{{"sha":"{sha}"}}"#,
                                      sha=sha);
            debug!("Updating ref with URL {} to {}", update_uri.as_str(), update_body.as_str());
            let ff_ref_request =
                Request::patch(Uri::from_shared(update_uri.into()).unwrap());
            let ff_ref_response =
                self.issue_request(ff_ref_request, Body::from(update_body))?;
            let ff_ref_response_status = ff_ref_response.status();

            if ff_ref_response_status.is_client_error() {
                debug!("Ref update failed with status {}, trying to auto-merge on the \
                        server", ff_ref_response_status.as_u16());
                let merge_uri = format!("{baseuri}/merges", baseuri=base_uri);
                let merge_body =
                    format!(r#"{{"base":"refs/heads/{base}","head":"{head}"}}"#,
                            base=ref_name,
                            head=sha);
                let merge_request =
                    Request::post(Uri::from_shared(merge_uri.into()).unwrap());
                self.issue_request(merge_request, Body::from(merge_body))
                    .and_then(|response| if response.status().is_success() {
                        let body = response.into_body();
                        let parsed_body =
                            from_json_slice::<restv3::GitMergeResponse>(body.as_ref())
                                .map_err(CacheError::JsonError)?;
                        sha_to_cache_ref(parsed_body.sha)
                            .map_err(|e| CacheError::Custom(
                                "Unable to parse cache ref of merge commit", e))
                    } else {
                        Err(CacheError::LayerError(
                            Error::GitOperationFailure(response.status()).into()))
                    })
            } else if ff_ref_response_status.is_success() {
                Ok(cache_ref)
            } else {
                Err(CacheError::LayerError(
                    Error::GitOperationFailure(ff_ref_response_status).into()))
            }
        } else {
            self.create_git_ref(branch, cache_ref)
                .map(|_| cache_ref)
        }
    }

    fn create_git_ref(&self, branch: &str, cache_ref: CacheRef) -> cache::Result<()> {
        // Ref doesn't exist yet, we POST it
        let uri = format!("{baseuri}/repos/{owner}/{repo}/git/refs",
                          baseuri = self.rest_base_uri,
                          owner = self.org,
                          repo = self.repo);
        let request = Request::post(Uri::from_shared(uri.into()).unwrap());
        let body = format!(r#"{{ "ref": "refs/heads/{gitref}", "sha": "{sha}" }}"#,
                           gitref = branch,
                           sha = cache_ref_to_sha(cache_ref.to_string()));
        self.issue_request(request, Body::from(body))
            .and_then(|response|
                if response.status() != StatusCode::CREATED {
                    Err(CacheError::LayerError(Error::GitOperationFailure(response.status())
                        .into()))
                } else {
                    Ok(())
                })
    }
}

#[cfg(test)]
mod test {
    use crate::cache::{CacheLayer, DirectoryEntry, ObjectType};
    use std::str::FromStr;
    use std::iter::FromIterator;
    use std::env;
    use std::fmt::Debug;
    use std::io::Write;
    use tempfile::NamedTempFile;
    use rand::{prelude::*, distributions::Alphanumeric};
    use chrono::Local;
    use super::*;

    fn setup_github() -> Github {
        let gh_token = env::var("GITHUB_TOKEN")
            .expect("Please set GITHUB_TOKEN to your GitHub token so that the test can access \
            darkwisebear/dorkfs");
        Github::new("https://api.github.com",
                    "darkwisebear",
                    "dorkfs",
                    gh_token.as_str()).unwrap()
    }

    #[test]
    fn get_github_commit() {
        crate::init_logging();
        let github = setup_github();
        let obj = github.get(&CacheRef::from_str("ccc13b55a0b2f41201e745a4bdc9a20bce19cce5000000000000000000000000").unwrap()).unwrap();
        debug!("Commit from GitHub: {:?}", obj);
    }

    #[test]
    fn get_github_tree() {
        crate::init_logging();
        let github = setup_github();
        let obj = github.get(&CacheRef::from_str("20325767a89a3f96949dee6f3cb29ad57f86c1c2000000000000000000000000").unwrap()).unwrap();
        debug!("Tree from GitHub: {:?}", obj);
    }

    #[test]
    fn get_github_blob() {
        crate::init_logging();
        let github = setup_github();
        let cache_ref = CacheRef::from_str("77bd95d183dbe757ebd53c0aa95d1a710b85460f000000000000000000000000").unwrap();
        let obj = github.get(&cache_ref).unwrap();
        debug!("Blob from GitHub: {:?}", obj);

        let explicit_get = github.get_blob_data(&cache_ref)
            .expect("Explicit get for blob failed");

        let mut contents = String::new();
        obj.into_file().unwrap().read_to_string(&mut contents).unwrap();
        assert_eq!(contents.into_bytes(), explicit_get);
    }

    #[test]
    fn empty_vs_missing_tree_entries() {
        use serde_json;
        use super::graphql::GitObject;
        // TODO: Add test that checks if present (but emtpy) tree entries are correctly deserialized
        // We expect this to be deserialized as Some(vec![])
        let test1 = r#"{ "__typename": "Tree", "entries": [] }"#;
        let test2 = r#"{ "__typename": "Tree", "entries": [{ "name": "test",
        "mode": 33188, "oid": "77bd95d183dbe757ebd53c0aa95d1a710b85460f", "object":
        { "__typename": "Blob", "isTruncated": false, "byteSize": 123 } }] }"#;
        let test3 = r#"{ "__typename": "Tree" }"#;

        let test1_parsed: GitObject = serde_json::from_str(test1).unwrap();
        let test2_parsed: GitObject = serde_json::from_str(test2).unwrap();
        let test3_parsed: GitObject = serde_json::from_str(test3).unwrap();

        if let GitObject::Tree { entries: Some(v) } = test1_parsed {
            assert_eq!(0, v.len());
        } else {
            panic!("Unexpected deserialization result: {:?}", test1_parsed);
        }

        if let GitObject::Tree { entries: Some(v) } = test2_parsed {
            assert_eq!("77bd95d183dbe757ebd53c0aa95d1a710b85460f", v[0].oid.oid.as_str());
            assert_eq!("test", v[0].name);
        } else {
            panic!("Unexpected deserialization result: {:?}", test3_parsed);
        }

        if let GitObject::Tree { entries: None } = test3_parsed {

        } else {
            panic!("Unexpected deserialization result: {:?}", test3_parsed);
        }
    }

    fn hash_map_from_dir<D>(dir: D)
        -> HashMap<String, DirectoryEntry> where D: Directory+Debug {
        dir.into_iter().map(|entry| (entry.name.clone(), entry)).collect()
    }

    #[test]
    fn change_file_and_commit() {
        use std::io::Write;

        crate::init_logging();
        let mut github = setup_github();
        let head_commit_ref = github.get_head_commit("test")
            .expect("Unable to get the head commit ref of the test branch")
            .expect("No head commit in test branch");
        let head_commit = github.get(&head_commit_ref)
            .and_then(CacheObject::into_commit)
            .expect("Unable to get the head commit of the test branch");
        let mut root_tree = github.get(&head_commit.tree)
            .and_then(CacheObject::into_directory)
            .map(hash_map_from_dir)
            .expect("Unable to get root tree of test branch");
        let mut src_tree = github.get(&root_tree.get("src")
            .expect("src dir not found in root tree").cache_ref)
            .expect("Unable to retrieve src dir")
            .into_directory().map(hash_map_from_dir)
            .expect("src is not a directory");

        let mut temp_file = ::tempfile::NamedTempFile::new().unwrap();
        writeln!(temp_file, "Test executed on {}", ::chrono::Local::now().to_rfc2822()).unwrap();
        let file_cache_ref = github.add_file_by_path(
            temp_file.into_temp_path().as_ref())
            .expect("Unable to upload file contents");
        src_tree.get_mut("hashfilecache.rs")
            .expect("Cannot find github.rs in src").cache_ref = file_cache_ref;

        let updated_dir = github.add_directory(
            &mut src_tree.into_iter().map(|(_, v)| v))
            .expect("Unable to upload updated src dir");
        root_tree.get_mut("src").unwrap().cache_ref = updated_dir;

        let updated_root = github.add_directory(
            &mut root_tree.into_iter().map(|(_, v)| v))
            .expect("Unable to upload updated root dir");

        let new_commit = crate::cache::Commit {
            tree: updated_root,
            parents: vec![head_commit_ref],
            message: "Test commit from unit test".to_string(),
            committed_date: Local::now()
        };

        let new_commit_ref = github.add_commit(new_commit)
            .and_then(|cache_ref| github.merge_commit("test", cache_ref))
            .expect("Unable to upload commit");
        assert_eq!(new_commit_ref, github.get_head_commit("test")
            .expect("Error getting new head commit")
            .expect("No new head commit"));
    }

    fn create_test_commit<R: Rng>(mut rng: R, gh: &Github, parent_commit: CacheRef)
        -> CacheRef {
        let ascii = rng.sample_iter(&Alphanumeric);

        let mut file1 = NamedTempFile::new().expect("Unable to open temp file");
        writeln!(file1, "Test1").expect("Unable to write to first file");
        let file1ref = gh.add_file_by_path(&file1.into_temp_path())
            .expect("Unable to add test file");
        let newdir1 = vec![DirectoryEntry {
            object_type: ObjectType::File,
            cache_ref: file1ref,
            size: 8,
            name: format!("test{}.txt", String::from_iter(ascii.take(8))) }];
        let newdir1ref = gh.add_directory(&mut newdir1.into_iter())
            .expect("Unable to generate new root directory");
        let newcommit1 = Commit {
            parents: vec![parent_commit],
            tree: newdir1ref,
            message: "Commit on top of the test branch".to_string(),
            committed_date: Local::now()
        };

        gh.add_commit(newcommit1)
            .expect("Unable to create first commit")
    }

    #[test]
    fn merge_concurrent_changes() {
        crate::init_logging();

        let mut gh = setup_github();
        let mut rng = StdRng::from_entropy();

        let current_head = gh.get_head_commit("mergetest")
            .expect("Unable to retrieve current HEAD")
            .expect("This test needs at least one root commit to work.");

        let commit1ref =
            create_test_commit(&mut rng, &gh, current_head);
        gh.merge_commit("mergetest", commit1ref)
            .expect("Unable to merge first commit to master");
        let commit2ref =
            create_test_commit(&mut rng, &gh, current_head);
        let final_ref = gh.merge_commit("mergetest", commit2ref)
            .expect("Unable to merge second commit to master");

        info!("Created parent commits {} and {}", commit1ref, commit2ref);

        let new_head = gh.get_head_commit("mergetest")
            .expect("Unable to get new HEAD commit ref")
            .unwrap();

        info!("New HEAD after merge is {}", new_head);
        assert_eq!(final_ref, new_head);

        let new_head_commit = gh.get(&new_head)
            .expect("Unable to fetch new HEAD commit contents")
            .into_commit()
            .expect("Retrieved HEAD object is not a commit");

        assert_eq!(2, new_head_commit.parents.len(), "New commit is not a merge commit");
        assert!(new_head_commit.parents.contains(&commit1ref) &&
                    new_head_commit.parents.contains(&commit2ref),
                "New merge commit does not contain the two commits created");
    }
}

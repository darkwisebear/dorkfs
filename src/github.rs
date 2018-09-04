use std::path::Path;
use std::io::{self, Read, Seek, SeekFrom, Cursor};
use std::vec;
use std::sync::{Arc, Weak, Mutex, mpsc::channel};
use std::str;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::default::Default;

use http::{uri::InvalidUri, request};
use hyper::{self, Uri, Chunk, Body, body::Payload, Request, Client, client:: HttpConnector};
use hyper_tls::{self, HttpsConnector};
use futures::{Stream, Future};
use tokio::{runtime::Runtime, self};
use serde_json::de::from_str as from_json_str;
use failure;
use base64;

use cache::{DirectoryEntry, ReadonlyFile, CacheObject, CacheObjectMetadata, CacheError, CacheRef,
            self, CacheLayer, Directory, LayerError, Commit};

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
    UnsupportedBlobEncoding
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
    base_uri: Uri,
    org: String,
    repo: String,
    token: String,
    tokio: Arc<Runtime>,
    http_client: Client<HttpsConnector<HttpConnector>>,
    query_cache: Mutex<HashMap<CacheRef, graphql::GitObject>>
}

mod restv3 {
    #[derive(Debug, Clone, Deserialize)]
    pub struct GitBlob {
        pub content: String,
        pub encoding: String,
        pub size: usize
    }
}

mod graphql {
    use std::str::FromStr;
    use std::io::Cursor;
    use std::fmt::{self, Formatter};
    use std::sync::Arc;

    use cache::{CacheObject, Commit, CacheRef, DirectoryEntry, ObjectType};
    use failure::Error;
    use serde::{Deserializer, de::Visitor};
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
        pub fn try_into_cache_ref(mut self) -> Result<CacheRef, Error> {
            self.oid.push_str("000000000000000000000000");
            CacheRef::from_str(self.oid.as_str())
        }
    }

    impl From<CacheRef> for GitObjectId {
        fn from(cache_ref: CacheRef) -> Self {
            let mut oid = cache_ref.to_string();
            oid.truncate(40);
            Self { oid }
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

    #[derive(Debug, Clone, Deserialize)]
    pub struct TreeEntry {
        pub name: String,
        pub mode: u32,
        #[serde(flatten)]
        pub oid: GitObjectId,
        pub object: GitObject
    }

    #[derive(Debug, Clone, Deserialize)]
    #[serde(tag = "__typename")]
    pub enum GitObject {
        Commit {
            author: Option<Author>,
            tree: Option<GitObjectId>,
            message: Option<String>,
            parents: Option<CommitConnection>,
            #[serde(flatten)]
            oid: Option<GitObjectId>
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
            entries: Vec<TreeEntry>
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

    pub fn object_to_object_type(object: &GitObject) -> ObjectType {
        match *object {
            GitObject::Commit { .. } => ObjectType::Commit,
            GitObject::Tree { .. } => ObjectType::Directory,
            GitObject::Blob { .. } => ObjectType::File
        }
    }

    impl Into<Result<CacheObject<GithubBlob, GithubTree>, Error>> for GitObject {
        fn into(self) -> Result<CacheObject<GithubBlob, GithubTree>, Error> {
            match self {
                GitObject::Commit {
                    tree: Some(tree),
                    parents: Some(parents),
                    message: Some(message), ..
                } => {
                    let cache_ref = tree.try_into_cache_ref()?;
                    let converted_parents =
                        parents.nodes.into_iter()
                            .map(|parent|
                                parent.try_into_cache_ref());
                    let commit = Commit {
                        tree: cache_ref,
                        parents: converted_parents.collect::<Result<Vec<CacheRef>, Error>>()?,
                        message
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
                    = entries.into_iter().map(|entry| -> Result<DirectoryEntry, Error> {Ok(DirectoryEntry {
                        name: entry.name,
                        cache_ref: entry.oid.try_into_cache_ref()?,
                        object_type: object_to_object_type(&entry.object),
                    })});
                    let dir_vec = dir.collect::<Result<_, Error>>()?;
                    Ok(CacheObject::Directory(GithubTree(dir_vec)))
                }
            }
        }
    }

    #[derive(Debug, Clone, Deserialize)]
    pub struct Ref {
        pub target: GitObject
    }

    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Repository {
        pub object: Option<GitObject>,
        pub default_branch_ref: Option<Ref>
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

    fn metadata(&self, cache_ref: &CacheRef) -> cache::Result<CacheObjectMetadata> {
        let object = self.query_object(cache_ref, false)?;
        let metadata = CacheObjectMetadata {
            object_type: graphql::object_to_object_type(
                &object),
            size: match object {
                graphql::GitObject::Blob { byte_size, .. } => byte_size as u64,
                graphql::GitObject::Tree { ref entries } => entries.len() as u64,
                graphql::GitObject::Commit { .. } => 0u64
            }
        };
        Ok(metadata)
    }

    fn add_file_by_path(&self, source_path: &Path) -> cache::Result<CacheRef> {
        unimplemented!()
    }

    fn add_directory(&self, items: &mut Iterator<Item=DirectoryEntry>) -> cache::Result<CacheRef> {
        unimplemented!()
    }

    fn add_commit(&self, commit: Commit) -> cache::Result<CacheRef> {
        unimplemented!()
    }

    fn get_head_commit(&self) -> cache::Result<Option<CacheRef>> {
        let query = format!("{{\"query\":\" \
query {{ \
  repository(owner:\\\"{org}\\\",name:\\\"{repo}\\\") {{ \
    defaultBranchRef {{ \
      target {{ \
        __typename \
        oid \
      }} \
    }} \
  }} \
}} \
\"}}", org=self.org, repo=self.repo);
        let response = self.execute_graphql_query(query)?;

        let head_commit_response: GraphQLQueryResponse = from_json_str(response.as_str())?;
        let head_commit_object = head_commit_response.data.repository.default_branch_ref
            .ok_or(Error::UnexpectedGraphQlResponse("Missing branch ref"))?
            .target;

        if let graphql::GitObject::Commit { oid: Some(oid), .. } = head_commit_object {
            oid.try_into_cache_ref()
                .map(Some)
                .map_err(|e| CacheError::Custom("Unparsable OID string", e))
        } else {
            Err(CacheError::UnexpectedObjectType(graphql::object_to_object_type(&head_commit_object)))
        }

    }
}

impl Github {
    fn push_chunk(mut string: String, data: Chunk) -> Result<String, Error> {
        let s = str::from_utf8(data.as_ref())?;
        string.push_str(s);
        Ok(string)
    }

    pub fn new(base_url: &str, org: &str, repo: &str, token: &str) -> Result<Self, Error> {
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

        Ok(Github {
            base_uri: base_url.parse()?,
            org: org.to_string(),
            repo: repo.to_string(),
            token: token.to_string(),
            tokio,
            http_client,
            query_cache: Default::default()
        })
    }

    pub fn get_blob_data(&self, cache_ref: &CacheRef) -> cache::Result<Vec<u8>> {
        let path = format!("/repos/{org}/{repo}/git/blobs/{sha}",
                           org=self.org,
                           repo=self.repo,
                           sha=&cache_ref.to_string()[0..40]);
        let mut uri_parts = self.base_uri.clone().into_parts();
        uri_parts.path_and_query = Some(path.parse().unwrap()); // TODO: Handle errors if this can fail
        let uri = Uri::from_parts(uri_parts).unwrap(); // TODO: Handle errors if this can fail

        info!("Retrieving git blob from {}", uri);

        let mut request_builder = Request::get(uri);
        request_builder.header("Accept", "application/vnd.github.v3+json");
        let response =
            self.issue_request(request_builder, Body::empty())?;

        // TODO: Explicitly handle the case when an object can't be found.
        // In case the object wasn't found (or isn't a blob) this call will fail with a weird
        // error. This should be handled explicitly.
        let git_blob: restv3::GitBlob = from_json_str(response.as_str())?;
        if git_blob.encoding.as_str() == "base64" {
            base64::decode_config(git_blob.content.as_str(), base64::MIME)
                .map_err(|e|
                    CacheError::Custom("Base64 decodong failed", e.into()))

        } else {
            Err(Error::UnsupportedBlobEncoding.into())
        }
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
}}", org = self.org, repo = self.repo, oid = &cache_ref.to_string()[0..40], get_contents=if get_blob_contents { "text" } else { "" });

        let response = self.execute_graphql_query(query)?;

        let mut get_response: GraphQLQueryResponse = from_json_str(response.as_str())?;
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
                self.ensure_blob_data(cache_ref, obj)?;
            }

            obj.clone()
        };

        // If we've received a tree we extract the entries so that we don't have to query them
        // again if we need their metadata
        if let graphql::GitObject::Tree { ref entries } = obj {
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

    fn ensure_blob_data(&self, cache_ref: &CacheRef, object: &mut graphql::GitObject)
        -> cache::Result<()> {
        if let graphql::GitObject::Blob {
                        ref mut text,
                        ref mut is_truncated, ..
                    } = object {
            if text.is_none() || *is_truncated {
                *text = Some(self.get_blob_data(cache_ref)?.into());
                *is_truncated = false;
            }
        }

        Ok(())
    }

    fn execute_graphql_query(&self, query: String) -> cache::Result<String> {
        let mut parts = self.base_uri.clone().into_parts();
        parts.path_and_query = Some("/graphql".parse().unwrap());
        let graphql_query_url = Uri::from_parts(parts).unwrap();

        debug!("Sending query to GitHub at {}: {}", graphql_query_url, query);

        let request_builder = Request::post(graphql_query_url);
        self.issue_request(request_builder, Body::from(query))
    }

    fn issue_request(&self, mut request_builder: request::Builder, body: Body)
        -> cache::Result<String> {
        let user_agent = format!("dorkfs/{}",
                                 option_env!("CARGO_PKG_VERSION").unwrap_or("(unknown)"));
        let request = request_builder
            .header("Authorization", format!("Bearer {}", &self.token).as_str())
            .header("User-Agent", user_agent)
            .body(body).unwrap();

        let (send, recv) = channel();

        let request_future =
            self.http_client.request(request)
                .map_err(Error::HyperError)
                .and_then(|response|
                    response.into_body()
                        .map_err(Error::HyperError)
                        .fold(String::new(), Self::push_chunk))
                .then(move |r|
                    send.send(r)
                        .map_err(|_|
                            unreachable!("Unexpected send error during GitHub request")));

        self.tokio.executor().spawn(request_future);
        let response =
            recv.recv().expect("Unexpected receive error during GitHub request")?;

        debug!("Raw response: {}", response);

        Ok(response)
    }
}

#[cfg(test)]
mod test {
    use cache::CacheLayer;
    use std::str::FromStr;
    use std::env;
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
        ::init_logging();
        let github = setup_github();
        let obj = github.get(&CacheRef::from_str("ccc13b55a0b2f41201e745a4bdc9a20bce19cce5000000000000000000000000").unwrap()).unwrap();
        debug!("Commit from GitHub: {:?}", obj);
    }

    #[test]
    fn get_github_tree() {
        ::init_logging();
        let github = setup_github();
        let obj = github.get(&CacheRef::from_str("20325767a89a3f96949dee6f3cb29ad57f86c1c2000000000000000000000000").unwrap()).unwrap();
        debug!("Tree from GitHub: {:?}", obj);
    }

    #[test]
    fn get_github_blob() {
        ::init_logging();
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
}

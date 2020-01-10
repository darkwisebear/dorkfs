mod restv3;
mod graphql;

use std::path::{PathBuf, Path};
use std::io::{self, Read, Seek, SeekFrom, Cursor};
use std::fs;
use std::sync::{Arc, Mutex};
use std::str::{self, FromStr};
use std::borrow::Cow;
use std::time::Duration;
use std::fmt::{Debug, Formatter, self};
use std::pin::Pin;
use std::env;

use http::{uri::InvalidUri, request};
use hyper::{self, Uri, Body, Request, Response, Client, StatusCode};
use hyper_tls;
use native_tls::{self, TlsConnector};
use hyper_proxy;
use typed_headers;
use futures::{self, Future, FutureExt, StreamExt, TryFutureExt, future};
use tokio;
use tokio_tls;
use serde::Deserialize;
use serde_json::{
    de::from_slice as from_json_slice,
    ser::to_string as to_json_string
};
use failure::{self, Fail, Fallible, format_err};
use base64;
use bytes::{Bytes, BytesMut};
use lru::LruCache;
use log::{debug, info, warn, error};

use crate::cache::{self, DirectoryEntry, ReadonlyFile, CacheObject, CacheError, CacheRef,
                   CacheLayer, LayerError, Commit};
use crate::utility::{RepoUrl, gitmodules::GitModules};

pub fn initialize_github_submodules<R: Read>(file: R,
                                       apiurl: &str,
                                       org: &str,
                                       token: &str)
    -> failure::Fallible<Vec<(PathBuf, RepoUrl<'static>)>> {
    let submodules = GitModules::from_reader(file)?;

    let cached_repos = submodules.into_iter()
        .filter_map(|submodule| {
            match submodule.url.get_github_submodule(apiurl, org) {
                Ok(github_submodule) => {
                    let baseurl = github_submodule.host;

                    info!("Adding submodule \"{}\" with url {} at path {} using base url https://{}",
                          &submodule.name, &submodule.url, submodule.path.display(), baseurl);

                    let github_submodule_url = crate::utility::RepoUrl::GithubHttps {
                        apiurl: Cow::Owned(baseurl.to_string()),
                        org: Cow::Owned(github_submodule.org.to_string()),
                        repo: Cow::Owned(github_submodule.repo.to_string()),
                        token: Cow::Owned(token.to_string())
                    };

                    Some((submodule.path, github_submodule_url))
                }

                Err(err) => {
                    warn!("Skipping submodule with unsupported URL {}: {}",
                          &submodule.url,
                          err);
                    None
                }
            }
        })
        .collect::<Vec<_>>();

    Ok(cached_repos)
}

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "Invalid GitHub URI {}", _0)]
    InvalidUri(InvalidUri),
    #[fail(display = "Unexpected GraphQL response from GitHub: {}", _0)]
    UnexpectedGraphQlResponse(&'static str),
    #[fail(display = "TLS connection error {}", _0)]
    TlsConnectionError(native_tls::Error),
    #[fail(display = "I/O error {}", _0)]
    IoError(tokio::io::Error),
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
    GitOperationFailure(StatusCode),
    #[fail(display = "JSON error: {}", _0)]
    JsonError(serde_json::Error),
    #[fail(display = "{}", _0)]
    Custom(failure::Error)
}

impl LayerError for Error {}

impl From<native_tls::Error> for Error {
    fn from(err: native_tls::Error) -> Self {
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
        Error::IoError(err)
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

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::JsonError(err)
    }
}

impl From<typed_headers::Error> for Error {
    fn from(err: typed_headers::Error) -> Self {
        Error::Custom(err.into())
    }
}

impl From<&'static str> for Error {
    fn from(err: &'static str) -> Self {
        Error::Custom(failure::err_msg(err))
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

#[derive(Debug, Clone)]
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

type GithubTree = Vec<DirectoryEntry>;
type GaiResolver = hyper::client::connect::dns::GaiResolver;
type HttpConnector = hyper::client::HttpConnector<GaiResolver>;
type HttpsConnector = hyper_tls::HttpsConnector<HttpConnector>;
type ProxyConnector = hyper_proxy::ProxyConnector<HttpsConnector>;

#[derive(Debug)]
struct GithubRequestBuilderImpl {
    graphql_uri: Uri,
    rest_base_uri: String,
    org: String,
    repo: String,
    token: String,
    http_client: Client<ProxyConnector>,
}

#[derive(Debug, Clone)]
struct GithubRequestBuilder(Arc<GithubRequestBuilderImpl>);

impl GithubRequestBuilder {
    async fn issue_request(&self, request_builder: request::Builder, body: Body)
        -> Result<Response<Bytes>, Error> {
        let user_agent = format!("dorkfs/{}",
                                 option_env!("CARGO_PKG_VERSION").unwrap_or("(unknown)"));
        let request = request_builder
            .header("Authorization",
                    format!("Bearer {}", &self.0.token).as_str())
            .header("User-Agent", user_agent)
            .body(body).unwrap();

        let response = self.0.http_client.request(request).await
            .map_err(Error::HyperError)?;
        let (parts, mut body) = response.into_parts();
        let mut bytes = BytesMut::new();
        loop {
            match body.next().await {
                Some(Ok(chunk)) => bytes.extend_from_slice(chunk.as_ref()),
                Some(Err(e)) => return Err(Error::from(e)),
                None => break
            }
        }
        Ok(Response::from_parts(parts, bytes.freeze()))
    }

    async fn execute_graphql_query(&self, query: String) -> Result<Response<Bytes>, Error> {
        let uri = self.0.graphql_uri.clone();
        debug!("Sending query to GitHub at {}: {}", &uri, query);

        let request_builder = Request::post(uri);
        self.issue_request(request_builder, Body::from(query)).await
    }

    async fn get_blob_data(&self, cache_ref: &CacheRef) -> Result<Vec<u8>, Error> {
        let path = format!("{base}/repos/{org}/{repo}/git/blobs/{sha}",
                           base = self.0.rest_base_uri,
                           org = self.0.org,
                           repo = self.0.repo,
                           sha = &cache_ref.to_string()[0..40]);
        // This shouldn't fail as we already checked the validity URL during construction
        let uri = Uri::from_maybe_shared(Bytes::from(path)).unwrap();

        info!("Retrieving git blob from {}", uri);

        let request_builder = Request::get(uri)
            .header("Accept", "application/vnd.github.v3+json");
        let response = self.issue_request(request_builder, Body::empty()).await?;
        // TODO: Explicitly handle the case when an object can't be found.
        // In case the object wasn't found (or isn't a blob) this call will fail with a
        // weird error. This should be handled explicitly.
        let git_blob: restv3::GitBlob = from_json_slice(response.body().as_ref())?;
        if git_blob.encoding.as_str() == "base64" {
            let mut content = git_blob.content.into_bytes();
            content.retain(|c| *c != b'\n');
            base64::decode_config(content.as_slice(), base64::STANDARD)
                .map_err(|e|
                    Error::Custom(format_err!("Base64 decodong failed: {}", e)))
        } else {
            Err(Error::UnsupportedBlobEncoding)
        }
    }

    async fn fetch_remote_object(&self, cache_ref: CacheRef, get_blob_contents: bool)
        -> Result<GraphQLQueryResponse, Error> {
        let get_contents = if get_blob_contents { "text" } else { "" };
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
        history(first:20) {{ \
          nodes {{ \
            oid \
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
}}", org = &self.0.org, repo = &self.0.repo, oid = &cache_ref.to_string()[0..40], get_contents = get_contents);

        self.execute_graphql_query(query)
            .await
            .and_then(move |response| {
                let mut get_response: GraphQLQueryResponse = from_json_slice(response.body().as_ref())?;

                // Set the oid since we didn't request it from the server as that would be redundant.
                if let Some(ref mut obj) = get_response.data.repository.object {
                    let new_oid = graphql::GitObjectId::from(cache_ref);
                    obj.set_oid(new_oid);
                }

                debug!("Parsed response: {:?}", get_response);

                Ok(get_response)
            })
    }

    async fn post_git_object_json(&self, obj_json: String, git_obj_type: &str)
                            -> Result<CacheRef, Error> {
        let path = format!("{base}/repos/{org}/{repo}/git/{obj_type}",
                           base = self.0.rest_base_uri,
                           org = self.0.org,
                           repo = self.0.repo,
                           obj_type = git_obj_type);
        // This shouldn't fail as we already checked the validity URL during construction
        let uri = Uri::from_maybe_shared(Bytes::from(path)).unwrap();

        let request_builder = Request::post(uri)
            .header("Accept", "application/vnd.github.v3+json");
        self.issue_request(request_builder, obj_json.into())
            .await
            .and_then(|response| {
                let git_blob_created: restv3::GitObjectReference =
                    from_json_slice(response.body().as_ref())?;

                sha_to_cache_ref(git_blob_created.sha)
                    .map_err(|e|
                        Error::Custom(format_err!("Cannot decode SHA reploy from GitHub after blob \
                        creation: {}", e)))
            })
    }

    async fn post_blob_data<R: Read>(&self, mut reader: R) -> Result<CacheRef, Error> {
        let mut input = Vec::new();
        let obj_json = reader.read_to_end(&mut input)
            .map_err(Into::into)
            .and_then(move |_| {
                let content = base64::encode_config(input.as_slice(), base64::CRYPT);
                let blob = restv3::GitBlob {
                    content,
                    encoding: "base64".to_string(),
                    size: None
                };

                to_json_string(&blob).map_err(Into::into)
            });

        match obj_json {
            Ok(obj_json) => self.post_git_object_json(obj_json, "blobs").await,
            Err(err) => Err(err)
        }
    }

    async fn ensure_git_object_data(&self, cache_ref: CacheRef, mut object: graphql::GitObject)
        -> Result<graphql::GitObject, Error> {
        let request_builder = self.clone();

        match object {
            graphql::GitObject::Blob {
                text: None,
                is_truncated: true, ..
            } => request_builder.get_blob_data(&cache_ref)
                .await
                .map(move |data| {
                    if let graphql::GitObject::Blob {
                        ref mut text,
                        ref mut is_truncated, .. } = object {
                        *text = Some(data.into());
                        *is_truncated = false;
                    } else {
                        unreachable!()
                    }

                    object
                }),

            // If the cached tree is missing its entries we add them directly to the cached
            // tree object
            graphql::GitObject::Tree { entries: None, oid: _ } => {
                request_builder.fetch_remote_object(cache_ref, false)
                    .await
                    .and_then(move |fetched_obj| {
                        object = fetched_obj.data.repository.object
                            .ok_or(Error::UnexpectedGraphQlResponse("Missing object"))?;

                        // Check if we received what we expected
                        match object {
                            graphql::GitObject::Tree { entries: Some(_), oid: _ } => Ok(object), // We're cool
                            graphql::GitObject::Tree { entries: None, oid: _ } =>
                                Err(Error::UnexpectedGraphQlResponse(
                                    "No entries received in tree")),
                            _ => {
                                error!("Unexpected object {:?}", object);
                                Err(Error::UnexpectedGraphQlResponse(
                                    "Unexpectedly not receiving a tree object"))
                            }
                        }
                    })
            }

            _ => Ok(object)
        }
    }

    async fn get_ref_info(&self, branch: Option<&str>) -> Result<Option<(String, CacheRef)>, Error> {
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
\"}}", ref_query = ref_query, org = self.0.org, repo = self.0.repo);
        let have_branch = branch.is_some();
        let branch = branch.unwrap_or("(default)").to_string();

        self.execute_graphql_query(query)
            .await
            .and_then(move |response| {
                if !response.status().is_success() {
                    Err(Error::UnknownReference(branch))
                } else {
                    Ok(response.into_body())
                }
            })
            .and_then(move |response| {
                let head_commit_response: GraphQLQueryResponse = from_json_slice(response.as_ref())?;

                let branch_ref = if have_branch {
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
                            .map_err(|e|
                                Error::Custom(format_err!("Unparsable OID string: {}", e)))
                    }

                    Some(graphql::Ref { target: graphql::GitObject::Tree { .. }, .. }) =>
                        Err(Error::UnexpectedGraphQlResponse("Requested commit. got tree")),

                    Some(graphql::Ref { target: graphql::GitObject::Blob { .. }, .. }) =>
                        Err(Error::UnexpectedGraphQlResponse("Requested commit. got blob")),

                    Some(_) =>
                        Err(Error::UnexpectedGraphQlResponse("No branch ref received")),

                    None => Ok(None)
                }
            })
    }

    async fn create_git_ref(&self, branch: &str, cache_ref: CacheRef) -> Result<(), Error> {
        // Ref doesn't exist yet, we POST it
        let uri = format!("{baseuri}/repos/{owner}/{repo}/git/refs",
                          baseuri = self.0.rest_base_uri,
                          owner = self.0.org,
                          repo = self.0.repo);
        let request = Request::post(Uri::from_maybe_shared(Bytes::from(uri)).unwrap());
        let body = format!(r#"{{ "ref": "refs/heads/{gitref}", "sha": "{sha}" }}"#,
                           gitref = branch,
                           sha = cache_ref_to_sha(cache_ref.to_string()));
        self.issue_request(request, Body::from(body))
            .await
            .and_then(|response|
                if response.status() != StatusCode::CREATED {
                    Err(Error::GitOperationFailure(response.status()))
                } else {
                    Ok(())
                })
    }

    async fn update_branch_head(&self, branch: &str, cache_ref: CacheRef)
        -> Result<CacheRef, Error> {
        let branch = Box::from(branch);
        let request_builder = self.clone();
        let ref_info = self.get_ref_info(Some(&branch)).await?;
        if let Some((ref_name, _)) = ref_info {
            // Ref exists, we need to issue a PATCH or a merge in case no fast-forward is
            // possible.
            let sha = cache_ref_to_sha(cache_ref.to_string());
            let base_uri = format!("{baseuri}/repos/{owner}/{repo}",
                                   baseuri=request_builder.0.rest_base_uri,
                                   owner=request_builder.0.org,
                                   repo=request_builder.0.repo);
            let update_uri = format!("{baseuri}/git/refs/heads/{gitref}",
                                     baseuri=base_uri,
                                     gitref=ref_name);
            let update_body = format!(r#"{{"sha":"{sha}"}}"#,
                                      sha=sha);
            debug!("Updating ref with URL {} to {}", update_uri.as_str(), update_body.as_str());
            let ff_ref_request =
                Request::patch(Uri::from_maybe_shared(Bytes::from(update_uri)).unwrap());
            let ff_ref_response = request_builder.issue_request(
                ff_ref_request,
                Body::from(update_body)).await?;

            let ff_ref_response_status = ff_ref_response.status();

            if ff_ref_response_status.is_client_error() {
                debug!("Ref update failed with status {}, trying to auto-merge on \
                the server", ff_ref_response_status.as_u16());
                let merge_uri = format!("{baseuri}/merges", baseuri=base_uri);
                let merge_body =
                    format!(r#"{{"base":"refs/heads/{base}","head":"{head}"}}"#,
                            base=ref_name,
                            head=sha);
                let merge_request =
                    Request::post(Uri::from_maybe_shared(Bytes::from(merge_uri)).unwrap());
                let response = request_builder.issue_request(
                    merge_request, Body::from(merge_body))
                    .await?;
                if response.status().is_success() {
                    let body = response.into_body();
                    let parsed_body =
                        from_json_slice::<restv3::GitMergeResponse>(body.as_ref())
                            .map_err(Error::JsonError)?;
                    sha_to_cache_ref(parsed_body.sha)
                        .map_err(|e|
                            Error::Custom(format_err!("Unable to parse cache \
                            ref of merge commit: {}", e)))
                } else {
                    Err(Error::GitOperationFailure(response.status()))
                }
            } else if ff_ref_response_status.is_success() {
                Ok(cache_ref)
            } else {
                Err(Error::GitOperationFailure(ff_ref_response_status))
            }
        } else {
            request_builder.create_git_ref(&branch, cache_ref).await?;
            Ok(cache_ref)
        }
    }
}

pub struct Github {
    request_builder: GithubRequestBuilder,
    object_cache: Arc<Mutex<LruCache<CacheRef, CacheObject<GithubBlob, GithubTree>>>>
}

impl Debug for Github {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        fmt.debug_struct("Github")
            .field("request_builder", &self.request_builder)
            // TODO: List cached objects
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize)]
struct GraphQLQueryResponse {
    data: graphql::Query
}

impl CacheLayer for Github {
    type File = GithubBlob;
    type Directory = GithubTree;
    // Replace with existential type once https://github.com/rust-lang/rfcs/pull/2515 lands.
    #[allow(clippy::type_complexity)]
    type GetFuture = Pin<Box<dyn Future<Output=Result<CacheObject<Self::File, Self::Directory>, CacheError>>+Send>>;

    fn get(&self, cache_ref: &CacheRef) -> cache::Result<CacheObject<Self::File, Self::Directory>> {
        let query_future = self.query_object(*cache_ref, true);
        self.execute_request(query_future)
    }

    fn add_file_by_path<P: AsRef<Path>>(&self, source_path: P) -> cache::Result<CacheRef> {
        let source = fs::File::open(source_path)?;
        let post_future = self.request_builder.post_blob_data(source);
        self.execute_request(post_future)
    }

    fn add_directory<I: IntoIterator<Item=DirectoryEntry>>(&self, items: I) -> cache::Result<CacheRef> {
        let git_tree_entries = items.into_iter().map(restv3::GitTreeEntry::from);
        let git_tree = restv3::GitTree {
            tree: git_tree_entries.collect(),
            base_tree: None
        };
        let git_tree_json = to_json_string(&git_tree)?;
        debug!("Adding directory with request {}", git_tree_json.as_str());
        let post_future = self.request_builder.post_git_object_json(git_tree_json, "trees");
        self.execute_request(post_future)
    }

    fn add_commit(&self, commit: Commit) -> cache::Result<CacheRef> {
        to_json_string(&restv3::GitCommit::from(commit))
            .map_err(Into::into)
            .and_then(|git_commit_json|
                self.execute_request(self.request_builder.post_git_object_json(git_commit_json, "commits")))
    }

    fn get_head_commit<S: AsRef<str>>(&self, branch: S) -> cache::Result<Option<CacheRef>> {
        self.get_ref_info(Some(branch.as_ref()))
            .map(|ref_info_result|
                ref_info_result.map(|(_, cache_ref)| cache_ref))
    }

    fn merge_commit<S: AsRef<str>>(&self, branch: S, cache_ref: &CacheRef) -> cache::Result<CacheRef> {
        let update_branch_future =
            self.request_builder.update_branch_head(branch.as_ref(), *cache_ref)
                .map_err(CacheError::from);
        self.execute_request(update_branch_future)
    }

    fn create_branch<S: AsRef<str>>(&self, branch: S, cache_ref: &CacheRef) -> cache::Result<()> {
        let create_git_ref_future =
            self.request_builder.create_git_ref(branch.as_ref(), *cache_ref)
                .map_err(CacheError::from);
        self.execute_request(create_git_ref_future)
    }

    fn get_poll(&self, cache_ref: &CacheRef) -> Self::GetFuture {
        let query_future = self.query_object(cache_ref.clone(), true);
        debug!("Size of future: {}", ::std::mem::size_of_val(&query_future));
        query_future.boxed()
    }
}

impl Github {
    pub fn new(base_url: &str, org: &str, repo: &str, token: &str)
               -> Result<Self, Error> {
        let tls_connector = tokio_tls::TlsConnector::from(TlsConnector::new()?);
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        let https = HttpsConnector::from((http, tls_connector));
        let proxied = Self::build_proxy_connector(https)?;
        let http_client = Client::builder().build(proxied);

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
            (format!("{}api/graphql", uri).parse().unwrap(), format!("{}api/v3", uri))
        };

        let request_builder = GithubRequestBuilderImpl {
            graphql_uri,
            rest_base_uri,
            org: org.to_string(),
            repo: repo.to_string(),
            token: token.to_string(),
            http_client,
        };

        Ok(Github {
            request_builder: GithubRequestBuilder(Arc::new(request_builder)),
            object_cache: Arc::new(Mutex::new(LruCache::new(8)))
        })
    }

    fn make_no_proxy_matcher() -> impl Fn(&str) -> bool {
        let mut excludes = Vec::new();
        match env::var("no_proxy") {
            Ok(value) => excludes.extend(value.split(',').map(str::to_string)),
            Err(env::VarError::NotPresent) => (),
            Err(e) => warn!("Unable to use no_proxy variable: {}. Ignoring.", e)
        }

        move |host|
            excludes.iter()
                .any(|entry| host.ends_with(entry.as_str()))
    }

    fn build_proxy_connector(connector: HttpsConnector) -> Result<ProxyConnector, Error> {
        let mut proxy_conn = ProxyConnector::new(connector)?;

        match env::var("https_proxy") {
            Ok(value) => {
                match Self::parse_https_proxy_conf(value) {
                    Ok(proxy) => proxy_conn.add_proxy(proxy),
                    Err(e) => warn!("Unable to parse contents of https_proxy: {}. Ignoring.", e)
                }
            },
            Err(env::VarError::NotPresent) => (),
            Err(e) => warn!("Unable to read https_proxy variable: {}. Ignoring.", e)
        }

        Ok(proxy_conn)
    }

    fn parse_https_proxy_conf(value: String) -> Fallible<hyper_proxy::Proxy> {
        let mut uri_parts = value.parse::<Uri>()?.into_parts();

        let auth_split = uri_parts.authority.as_ref()
            .and_then(|auth|
                auth.as_str().find('@')
                    .map(|at| auth.as_str().split_at(at)));
        let (userpass, new_auth) = match auth_split {
            Some((userpass, host)) => {
                let new_auth = http::uri::Authority::from_str(&host[1..]).unwrap();
                (Some(userpass), Some(new_auth))
            }
            None => (None, uri_parts.authority)
        };

        let cred = match userpass {
            Some(userpass) => {
                let colon = userpass.find(':')
                    .ok_or_else(|| Error::from("Unable to find password inside credentials."))?;
                let (user, pass) = userpass.split_at(colon);
                Some(typed_headers::Credentials::basic(user, &pass[1..])?)
            }
            None => None
        };

        uri_parts.authority = new_auth;
        let uri = Uri::from_parts(uri_parts).unwrap();

        let matches_no_proxy = Self::make_no_proxy_matcher();
        let intercept = move |scheme: Option<&str>, host: Option<&str>, _|
            scheme == Some("https") && host.map(|h| !matches_no_proxy(h)).unwrap_or(true);

        let mut proxy = hyper_proxy::Proxy::new(intercept, uri);
        if let Some(cred) = cred {
            proxy.set_authorization(cred);
        }

        Ok(proxy)
    }

    pub fn get_default_branch(&self) -> cache::Result<String> {
        self.get_ref_info(None)
            .and_then(|ref_info| ref_info
                .ok_or_else(|| Error::UnknownReference("(default branch)".to_string()).into())
                .map(|(ref_name,_)| ref_name))
    }

    // This function isn't async itself since we want to first extract some data in the same thread
    // so that we do not run into lifetime problems.
    fn query_object(&self, cache_ref: CacheRef, get_blob_contents: bool)
        -> impl Future<Output=Result<CacheObject<GithubBlob, GithubTree>, CacheError>> {
        let cache_obj = self.object_cache.lock().unwrap().get(&cache_ref).cloned();

        if let Some(obj) = cache_obj {
            future::ok(obj).left_future()
        } else {
            let request_builder = self.request_builder.clone();
            let object_cache = Arc::clone(&self.object_cache);
            async move {
                let base_obj = request_builder.fetch_remote_object(cache_ref, get_blob_contents).await?;
                let obj = base_obj.data.repository.object
                    .ok_or(Error::UnexpectedGraphQlResponse("Missing object"))
                    .map_err(CacheError::from)?;
                let obj_with_contents = if get_blob_contents {
                    request_builder.ensure_git_object_data(cache_ref, obj).await?
                } else {
                    obj
                };

                let mut result = Err(CacheError::ObjectNotFound(cache_ref));
                for cache_obj in obj_with_contents.clone() {
                    let (obj_cache_ref, cache_obj) = cache_obj
                        .map_err(|e|
                            CacheError::Custom("Unable to parse graphql response", e))?;
                    if cache_ref == obj_cache_ref {
                        result = Ok(cache_obj.clone());
                    }
                    debug!("Add {} to github cache", obj_cache_ref);
                    object_cache.lock().unwrap().put(obj_cache_ref, cache_obj);
                }

                result
            }.right_future()
        }
    }

    fn execute_request<T, E, F>(&self, request_future: F) -> cache::Result<T>
        where T: Send+'static,
              E: Into<CacheError>+Send+'static,
              F: Future<Output=Result<T, E>>+Send {
        let request = tokio::time::timeout(Duration::from_secs(60), request_future)
            .map(|e| match e {
                Ok(inner) => inner.map_err(Into::into),
                Err(_) => Err(CacheError::from(Error::TimeoutError))
            });
        futures::executor::block_on(request)
    }

    fn get_ref_info(&self, branch: Option<&str>) -> Result<Option<(String, CacheRef)>, CacheError> {
        let get_ref_info_future =
            self.request_builder.get_ref_info(branch)
                .map_err(CacheError::from);
        self.execute_request(get_ref_info_future)
    }
}

#[cfg(test)]
mod test {
    use crate::cache::{CacheLayer, DirectoryEntry, ObjectType, Directory};
    use std::str::FromStr;
    use std::iter::FromIterator;
    use std::env;
    use std::fmt::Debug;
    use std::io::Write;
    use std::collections::HashMap;
    use std::sync::{Arc, Weak, Mutex};
    use tempfile::NamedTempFile;
    use rand::{prelude::*, distributions::Alphanumeric};
    use chrono::{Local, FixedOffset};
    use tokio::{self, runtime::Runtime};
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

    #[tokio::test(threaded_scheduler)]
    async fn get_github_commit() {
        crate::init_logging();
        let github = setup_github();
        let obj = github.get(&CacheRef::from_str("ccc13b55a0b2f41201e745a4bdc9a20bce19cce5000000000000000000000000").unwrap()).unwrap();
        let commit = obj.into_commit().expect("Unable to convert into commit");
        debug!("Commit from GitHub: {:?}", commit);
    }

    #[tokio::test(threaded_scheduler)]
    async fn get_github_tree() {
        crate::init_logging();
        let github = setup_github();
        let obj = github.get(&CacheRef::from_str("20325767a89a3f96949dee6f3cb29ad57f86c1c2000000000000000000000000").unwrap()).unwrap();
        debug!("Tree from GitHub: {:?}", obj);
    }

    #[tokio::test(threaded_scheduler)]
    async fn get_github_blob() {
        crate::init_logging();
        let github = setup_github();
        let cache_ref = CacheRef::from_str("77bd95d183dbe757ebd53c0aa95d1a710b85460f000000000000000000000000").unwrap();
        let obj = github.get(&cache_ref).unwrap();
        debug!("Blob from GitHub: {:?}", obj);

        let explicit_get = github.execute_request(github.request_builder.get_blob_data(&cache_ref))
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

        if let GitObject::Tree { entries: Some(v), oid: _ } = test1_parsed {
            assert_eq!(0, v.len());
        } else {
            panic!("Unexpected deserialization result: {:?}", test1_parsed);
        }

        if let GitObject::Tree { entries: Some(v), oid: _ } = test2_parsed {
            assert_eq!("77bd95d183dbe757ebd53c0aa95d1a710b85460f", v[0].oid.oid.as_str());
            assert_eq!("test", v[0].name);
        } else {
            panic!("Unexpected deserialization result: {:?}", test3_parsed);
        }

        if let GitObject::Tree { entries: None, oid: _ } = test3_parsed {

        } else {
            panic!("Unexpected deserialization result: {:?}", test3_parsed);
        }
    }

    fn hash_map_from_dir<D>(dir: D)
        -> HashMap<String, DirectoryEntry> where D: Directory+Debug {
        dir.into_iter().map(|entry| (entry.name.clone(), entry)).collect()
    }

    #[tokio::test(threaded_scheduler)]
    async fn change_file_and_commit() {
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
            temp_file.into_temp_path())
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
            committed_date: Local::now().with_timezone(&FixedOffset::east(0))
        };

        let new_commit_ref = github.add_commit(new_commit)
            .and_then(|cache_ref| github.merge_commit("test", &cache_ref))
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
            committed_date: Local::now().with_timezone(&FixedOffset::east(0))
        };

        gh.add_commit(newcommit1)
            .expect("Unable to create first commit")
    }

    #[tokio::test(threaded_scheduler)]
    async fn merge_concurrent_changes() {
        crate::init_logging();
        let mut gh = setup_github();
        let mut rng = StdRng::from_entropy();

        let current_head = gh.get_head_commit("mergetest")
            .expect("Unable to retrieve current HEAD")
            .expect("This test needs at least one root commit to work.");

        let commit1ref =
            create_test_commit(&mut rng, &gh, current_head);
        gh.merge_commit("mergetest", &commit1ref)
            .expect("Unable to merge first commit to master");
        let commit2ref =
            create_test_commit(&mut rng, &gh, current_head);
        let final_ref = gh.merge_commit("mergetest", &commit2ref)
            .expect("Unable to merge second commit to master");

        info!("Created parent commits {} and {}", commit1ref, &commit2ref);

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

    #[test]
    fn parse_link_response() {
        let testresponse = r#"{
"data": {
    "repository": {
        "object":
            {
                "__typename":"Blob",
                "text":"README.md",
                "byteSize":9,
                "isTruncated":false
            }
        }
    }
}"#;
        let mut parsed_response =
            serde_json::from_str::<GraphQLQueryResponse>(testresponse).unwrap();
        if let Some(graphql::GitObject::Blob { text, .. }) = parsed_response.data.repository.object {
            let bytes = text.unwrap();
            let content = str::from_utf8(bytes.as_ref()).unwrap();
            assert_eq!("README.md", content);
        } else {
            panic!("No object found in response: {:?}", parsed_response.data.repository.object);
        }
    }
}

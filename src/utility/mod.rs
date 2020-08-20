pub mod gitmodules;
pub mod diff;
mod gitconfig;

use std::ffi::OsString;
use std::fmt::Debug;
use std::borrow::Cow;
use std::str::FromStr;
use std::path::{Component, Path};
use std::env;

use failure::{format_err, bail, Fallible};

pub fn os_string_to_string(s: OsString) -> Fallible<String> {
    s.into_string()
        .map_err(|s| {
            format_err!("Unable to convert {} into UTF-8", s.to_string_lossy())
        })
}

#[derive(Clone, Debug)]
pub enum RepoUrl<'a> {
    GithubHttps {
        apiurl: Cow<'a, str>,
        org: Cow<'a, str>,
        repo: Cow<'a, str>,
        token: Cow<'a, str>,
    },

    GitFile {
        path: Cow<'a, Path>
    }
}

impl FromStr for RepoUrl<'static> {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let borrowed_repo_url = RepoUrl::from_borrowed_str(s)?;
        match borrowed_repo_url {
            RepoUrl::GithubHttps {
                apiurl, org, repo, token
            } => {
                let apiurl = apiurl.into_owned();
                let org = org.into_owned();
                let repo = repo.into_owned();
                let token = token.into_owned();
                Ok(RepoUrl::GithubHttps {
                    apiurl: Cow::Owned(apiurl),
                    org: Cow::Owned(org),
                    repo: Cow::Owned(repo),
                    token: Cow::Owned(token)
                })
            }

            RepoUrl::GitFile { path } => Ok(RepoUrl::GitFile {
                path: Cow::Owned(path.into_owned())
            })
        }
    }
}

impl<'a> RepoUrl<'a> {
    pub fn from_borrowed_str(repo: &'a str) -> Fallible<Self> {
        let (scheme, remainder) = Self::split_scheme(repo)?;
        match scheme {
            "github+https" => {
                let mut splitter = remainder.rsplitn(3, '/');
                let repo = splitter.next()
                    .ok_or_else(|| format_err!("Repo missing in repo URL"))?;
                let org = splitter.next()
                    .ok_or_else(|| format_err!("Org/user missing in repo URL"))?;
                let hostname = splitter.next()
                    .ok_or_else(|| format_err!("Api URL missing in repo URL"))?;
                let (token, apiurl) = if let Some(pos) = hostname.find('@') {
                    (Cow::Borrowed(&hostname[..pos]), Cow::Borrowed(&hostname[pos+1..]))
                } else {
                    let token = env::var("GITHUB_TOKEN").map_err(|_|
                        failure::err_msg("Github token must be given either in the URL \
                        before the hostname, separated by an '@' or in the environment as \
                        GITHUB_TOKEN"))?;
                    (Cow::Owned(token), Cow::Borrowed(hostname))
                };
                Ok(RepoUrl::GithubHttps {
                    apiurl,
                    org: Cow::Borrowed(org),
                    repo: Cow::Borrowed(repo),
                    token
                })
            }

            "git+file" => Ok(RepoUrl::GitFile {
                path: Cow::Borrowed(Path::new(remainder))
            }),

            unknown_scheme => bail!("Unknown repo URL scheme {}", unknown_scheme)
        }
    }

    fn split_scheme(repo: &str) -> Fallible<(&str, &str)> {
        repo.find(':').ok_or_else(|| format_err!("Missing scheme in repo URL"))
            .and_then(|pos| repo.get(pos+3..)
                .ok_or_else(|| format_err!("Incomplete repo URL"))
                .map(|path| (&repo[..pos], path)))
    }

    pub fn shortname(&self) -> impl AsRef<str> {
        match self {
            RepoUrl::GithubHttps {
                org, repo, ..
            } => format!("GitHub {}/{}", org, repo),

            RepoUrl::GitFile { path } =>
                format!("Git {}", path.components().next_back()
                    .map(|c| <Component as AsRef<Path>>::as_ref(&c).display().to_string())
                    .unwrap_or_default())
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        path::Path,
        env
    };

    use super::RepoUrl;

    #[test]
    fn parse_github_url() {
        env::set_var("GITHUB_TOKEN", "example");
        let repo_parts =
            RepoUrl::from_borrowed_str("github+https://api.github.com/darkwisebear/dorkfs")
                .expect("Unable to parse repo URL");
        match repo_parts {
            RepoUrl::GithubHttps {
                apiurl,
                org,
                repo,
                token
            } => {
                assert_eq!("api.github.com", apiurl);
                assert_eq!("darkwisebear", org);
                assert_eq!("dorkfs", repo);
                assert_eq!("example", token);
            }

            wrong_parse => panic!("Repo URL incorrectly parsed: {:?}", wrong_parse)
        }
    }

    #[test]
    fn parse_on_premises_url() {
        env::set_var("GITHUB_TOKEN", "example");
        let repo_parts =
            RepoUrl::from_borrowed_str("github+https://github.example.com/api/darkwisebear/dorkfs")
                .expect("Unable to parse repo URL");
        match repo_parts {
            RepoUrl::GithubHttps {
                apiurl,
                org,
                repo,
                token
            } => {
                assert_eq!("github.example.com/api", apiurl);
                assert_eq!("darkwisebear", org);
                assert_eq!("dorkfs", repo);
                assert_eq!("example", token);
            }

            wrong_parse => panic!("Repo URL incorrectly parsed: {:?}", wrong_parse)
        }
    }

    #[test]
    fn parse_token_in_github_url() {
        env::set_var("GITHUB_TOKEN", "example");
        let repo_parts =
            RepoUrl::from_borrowed_str("github+https://token@github.example.com/api/darkwisebear/dorkfs")
                .expect("Unable to parse repo URL");
        match repo_parts {
            RepoUrl::GithubHttps {
                apiurl,
                org,
                repo,
                token
            } => {
                assert_eq!("github.example.com/api", apiurl);
                assert_eq!("darkwisebear", org);
                assert_eq!("dorkfs", repo);
                assert_eq!("token", token);
            }

            wrong_parse => panic!("Repo URL incorrectly parsed: {:?}", wrong_parse)
        }
    }

    #[test]
    fn parse_local_git_repo() {
        let repo_parts =
            RepoUrl::from_borrowed_str("git+file:///tmp/path/to/repo.git")
                .expect("Unable to parse repo URL");
        match repo_parts {
            RepoUrl::GitFile { path } =>
                assert_eq!(Path::new("/tmp/path/to/repo.git"), path),

            wrong_parse => panic!("Repo URL incorrectly parsed: {:?}", wrong_parse)
        }
    }
}

use std::collections::hash_map::{HashMap, self};
use std::path::PathBuf;
use std::io::Read;
use regex::Regex;
use std::fmt::{self, Display, Formatter};
use std::iter;

use failure::{format_err, Fallible};
use lazy_static::lazy_static;

use super::gitconfig::{GitConfig, Section};

lazy_static! {
    static ref HTTPS_URL_PARSER: Regex = Regex::new(r"https://(.+)/(.+)/([^.]+)(?:\.git)?").unwrap();
    static ref SSH_URL_PARSER: Regex = Regex::new(r"git@(.+):(.+)/([^.]+)(?:\.git)?").unwrap();
    static ref ORG_REL_URL_PARSER: Regex = Regex::new(r"\.\./(.+)").unwrap();
    static ref SERVER_REL_URL_PARSER: Regex = Regex::new(r"\.\./\.\./(.+)/(.+)").unwrap();
}

#[derive(Debug, Clone)]
pub struct GitUrl {
    url: String
}

impl Display for GitUrl {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        <String as Display>::fmt(&self.url, f)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GithubSubmodule<'a> {
    pub host: &'a str,
    pub org: &'a str,
    pub repo: &'a str
}

impl GitUrl {
    pub fn get_github_submodule<'a>(&'a self, parent_hostname: &'a str, parent_org: &'a str)
        -> Fallible<GithubSubmodule<'a>> {
        if let Some(captures) = HTTPS_URL_PARSER.captures(&self.url) {
            Ok(GithubSubmodule {
                host: captures.get(1).unwrap().as_str(),
                org: captures.get(2).unwrap().as_str(),
                repo: captures.get(3).unwrap().as_str()
            })
        } else if let Some(captures) = SSH_URL_PARSER.captures(&self.url) {
            Ok(GithubSubmodule {
                host: captures.get(1).unwrap().as_str(),
                org: captures.get(2).unwrap().as_str(),
                repo: captures.get(3).unwrap().as_str()
            })
        } else if let Some(captures) = SERVER_REL_URL_PARSER.captures(&self.url) {
            Ok(GithubSubmodule {
                host: parent_hostname,
                org: captures.get(1).unwrap().as_str(),
                repo: captures.get(2).unwrap().as_str()
            })
        } else if let Some(captures) = ORG_REL_URL_PARSER.captures(&self.url) {
            Ok(GithubSubmodule {
                host: parent_hostname,
                org: parent_org,
                repo: captures.get(1).unwrap().as_str()
            })
        } else {
            Err(format_err!("Given URL {} is not a Github submodule", &self.url))
        }
    }
}

impl AsRef<str> for GitUrl {
    fn as_ref(&self) -> &str {
        self.url.as_str()
    }
}

#[derive(Debug, Clone)]
pub struct GitModule {
    pub name: String,
    pub url: GitUrl,
    pub path: PathBuf
}

#[derive(Debug, Clone)]
pub struct GitModules(HashMap<String, GitModule>);

impl GitModules {
    pub fn from_reader<R: Read>(reader: R) -> Fallible<Self> {
        GitConfig::from_reader(reader)
            .and_then(Self::try_from)
    }

    pub fn try_from(config: GitConfig) -> Fallible<Self> {
        fn config_to_submodule(subsection_name: Option<String>, mut section: Section)
            -> Fallible<(String, GitModule)> {
            let name = subsection_name.ok_or_else(||
                format_err!("Submodule without name in .gitmodules"))?;
            let url = section.extract_string("url").ok_or_else(||
                format_err!("Submodule {} doesn't contain a URL", name.as_str()))?.to_string();
            let path = section.extract_path("path").ok_or_else(||
                format_err!("Submodule {} doesn't contain a path", name.as_str()))?;
            let module = GitModule {
                name: name.clone(),
                url: GitUrl { url },
                path
            };

            Ok((name, module))
        }

        let modules = config.into_iter()
            .filter_map(|(section_name, subsection_name, section)| {
                if section_name.as_str() == "submodule" {
                    Some(config_to_submodule(subsection_name, section))
                } else {
                    None
                }
            })
            .collect::<Fallible<_>>()?;

        Ok(GitModules(modules))
    }

    pub fn get(&self, name: &str) -> Option<&GitModule> {
        self.0.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item=&GitModule> {
        <&Self as IntoIterator>::into_iter(self)
    }
}

impl<'a> IntoIterator for &'a GitModules {
    type Item = &'a GitModule;
    type IntoIter = hash_map::Values<'a, String, GitModule>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.values()
    }
}

type Extract2nd<U, V> = fn((U, V)) -> V;
type Extract2ndFromHashIter<U, V> = iter::Map<hash_map::IntoIter<U, V>, Extract2nd<U, V>>;

impl IntoIterator for GitModules {
    type Item = GitModule;
    type IntoIter = Extract2ndFromHashIter<String, GitModule>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter().map(|(_, module)| module)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::Path;

    #[test]
    fn parse_gitmodules() {
        let sample = br#"[submodule "test1"]
path = sub1
url = https://github.com/darkwisebear/dorkfs

[submodule "test2"]
path = sub/path
url = git@github.com:linux/kernel.git

[submodule "orgrel"]
path = sub2/path2
url = ../serde_urlencode

[submodule "serverrel"]
path = sub2/path3
url = ../../rust-lang/rust

[submodule "notagithubthing"]
path = sub3/path
url = ssh://192.168.2.1:test/git
"#;

        let gitmodules = GitModules::from_reader(&sample[..]).unwrap();
        assert_eq!(Path::new("sub1"), gitmodules.get("test1").unwrap().path.as_path());
        let test2 = gitmodules.get("test2").unwrap();
        assert_eq!("git@github.com:linux/kernel.git", test2.url.as_ref());
        let test2_mod =
            test2.url.get_github_submodule("github.com", "darkwisebear")
                .unwrap();
        assert_eq!(test2_mod, GithubSubmodule {
            host: "github.com",
            org: "linux",
            repo: "kernel"
        });

        assert_eq!(gitmodules.get("test1").unwrap()
                       .url.get_github_submodule("github.example.com",
                                                 "someorg")
                       .unwrap(),
                   GithubSubmodule {
                       host: "github.com",
                       org: "darkwisebear",
                       repo: "dorkfs"
                   }
        );

        assert_eq!(gitmodules.get("orgrel").unwrap()
                       .url.get_github_submodule("github.com",
                                                 "darkwisebear")
                       .unwrap(),
                   GithubSubmodule {
                       host: "github.com",
                       org: "darkwisebear",
                       repo: "serde_urlencode"
                   }
        );

        assert_eq!(gitmodules.get("serverrel").unwrap()
                       .url.get_github_submodule("github.com",
                                                 "darkwisebear")
                       .unwrap(),
                   GithubSubmodule {
                       host: "github.com",
                       org: "rust-lang",
                       repo: "rust"
                   }
        );

        gitmodules.get("notagithubthing").unwrap()
            .url.get_github_submodule("github.com", "darkwisebear")
            .unwrap_err();
    }
}

use std::collections::HashMap;
use std::path::PathBuf;
use std::io::Read;

use failure::Fallible;

use super::gitconfig::{GitConfig, Section};

#[derive(Debug, Clone)]
pub struct GitModule {
    pub name: String,
    pub url: String,
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
                url,
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
url = git@github.com:linux/kernel
"#;

        let gitmodules = GitModules::from_reader(&sample[..]).unwrap();
        assert_eq!(Path::new("sub1"), gitmodules.get("test1").unwrap().path.as_path());
        assert_eq!("git@github.com:linux/kernel", gitmodules.get("test2").unwrap().url);
    }
}

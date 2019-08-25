use std::hash::{Hash, Hasher};
use std::borrow::{Cow, Borrow};
use std::mem::replace;
use std::collections::hash_map::{self, HashMap};
use std::path::PathBuf;
use std::io::{Read, BufReader, BufRead};
use std::env;
use std::str::FromStr;

use failure::Fallible;
use regex::Regex;

lazy_static! {
    static ref SECTION_PARSER: Regex = Regex::new(r#"\[([[[:alnum:]]-.]+)(?:\s+"(.+)")?\s*]"#)
        .unwrap();
}

#[derive(Clone, Debug)]
struct SectionKey(Box<str>, Option<Box<str>>);
#[derive(Clone, Debug, PartialEq)]
struct BorrowedSectionKey<'a>(&'a str, Option<&'a str>);

impl Hash for SectionKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        <Self as SectionDescriptor>::hash(self, state);
    }
}

impl PartialEq for SectionKey {
    fn eq(&self, other: &SectionKey) -> bool {
        self.to_descriptor().eq(&other.to_descriptor())
    }
}

impl Eq for SectionKey {}

trait SectionDescriptor {
    fn to_descriptor(&self) -> BorrowedSectionKey;
    fn hash<H: Hasher>(&self, state: &mut H) where Self: Sized {
        let desc = self.to_descriptor();
        desc.0.hash(state);
        desc.1.hash(state);
    }
}

impl SectionDescriptor for SectionKey {
    fn to_descriptor(&self) -> BorrowedSectionKey {
        let s = self.1.as_ref().map(|s| s.as_ref());
        BorrowedSectionKey(self.0.as_ref(), s)
    }
}

impl<'a> SectionDescriptor for BorrowedSectionKey<'a> {
    fn to_descriptor(&self) -> BorrowedSectionKey {
        self.clone()
    }
}

impl<'a> Borrow<dyn SectionDescriptor + 'a> for SectionKey {
    fn borrow(&self) -> &(dyn SectionDescriptor + 'a) {
        self as &dyn SectionDescriptor
    }
}

impl<'a> PartialEq for (SectionDescriptor + 'a) {
    fn eq(&self, other: &dyn SectionDescriptor) -> bool {
        let lhs = self.to_descriptor();
        let rhs = other.to_descriptor();
        lhs.0 == rhs.0 && lhs.1 == rhs.1
    }
}

impl<'a> Eq for (SectionDescriptor + 'a) {}

impl<'a> Hash for (SectionDescriptor + 'a) {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_descriptor().hash(state);
    }
}

#[derive(Debug, Clone)]
pub struct Section(HashMap<String, String>);

impl Section {
    pub fn get_bool(&self, name: &str) -> Option<Fallible<bool>> {
        self.0.get(name)
            .map(|val| match val.as_str() {
                "yes" | "on" | "true" | "1" | "" => Ok(true),
                "no" | "off" | "false" | "0" => Ok(false),
                _ => bail!("Unable to parse {} as bool", val.as_str())
            })
    }

    pub fn get_integer(&self, name: &str) -> Option<Fallible<isize>> {
        self.0.get(name)
            .map(|val| isize::from_str(val.as_str())
                .map_err(Into::into))
    }

    pub fn get_path(&self, name: &str) -> Option<PathBuf> {
        let val = self.0.get(name)?;
        if val.starts_with("~/") || val.starts_with("~user/") {
            let mut path = PathBuf::from(env::var_os("HOME")?);
            let subpath = &val[val.find('/').unwrap()+1..];
            path.push(subpath);
            Some(path)
        } else {
            Some(PathBuf::from(val))
        }
    }

    pub fn get_str(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(|s| s.as_str())
    }

    pub fn extract_path(&mut self, name: &str) -> Option<PathBuf> {
        let val = self.0.remove(name)?;
        if val.starts_with("~/") || val.starts_with("~user/") {
            let mut path = PathBuf::from(env::var_os("HOME")?);
            let subpath = &val[val.find('/').unwrap()+1..];
            path.push(subpath);
            Some(path)
        } else {
            Some(PathBuf::from(val))
        }
    }

    pub fn extract_string(&mut self, name: &str) -> Option<String> {
        self.0.remove(name)
    }
}

#[derive(Debug, Clone)]
pub struct GitConfig(HashMap<SectionKey, Section>);

impl GitConfig {
    fn add_section(&mut self,
                   section_name: Box<str>,
                   subsection_name: Option<Box<str>>,
                   section: HashMap<String, String>) {
        let section_key = SectionKey(section_name, subsection_name);
        self.0.insert(section_key, Section(section));
    }

    fn decode_subsection(subsection: &str) -> String {
        let mut escape = false;
        let mut result = String::with_capacity(subsection.len());
        for c in subsection.chars() {
            if !escape {
                if c == '\\' {
                    escape = true;
                } else {
                    result.push(c);
                }
            } else {
                escape = false;
                result.push(c);
            }
        }
        result
    }

    pub fn iter(&self) -> SectionIter {
        SectionIter(self.0.iter())
    }

    pub fn into_iter(self) -> IntoSectionIter {
        IntoSectionIter(self.0.into_iter())
    }

    pub fn from_reader<R: Read>(reader: R) -> Fallible<Self> {
        let reader = BufReader::new(reader);
        let mut config = GitConfig(HashMap::new());
        let mut current_section_name: Option<Box<str>> = None;
        let mut current_subsection_name: Option<Box<str>> = None;
        let mut current_section = HashMap::new();
        for line in reader.lines() {
            let line = line?;
            let line = line.split(|s| s == '#' || s == ';').next().unwrap().trim_start();
            if line.starts_with('[') {
                // Store the old section
                if let Some(current_section_name) = current_section_name.take() {
                    config.add_section(current_section_name,
                                       current_subsection_name.take(),
                                       replace(&mut current_section,
                                               HashMap::new()))
                }

                let section_captures = SECTION_PARSER.captures(line)
                    .ok_or_else(|| format_err!("Cannot parse section header {}", line))?;
                current_section_name = section_captures.get(1)
                    .map(|m| m.as_str().to_lowercase().into_boxed_str());
                current_subsection_name = section_captures.get(2)
                    .map(|m|
                        Self::decode_subsection(m.as_str()).into_boxed_str());
            } else if let Some(equal_index) = line.find('=') {
                let key = line[0..equal_index].trim_end();
                let value = line[equal_index+1..].trim();
                current_section.insert(key.to_string(), value.to_string());
            } else {
                current_section.insert(line.trim_end().to_string(), "1".to_string());
            }
        }

        if let Some(current_section_name) = current_section_name.take() {
            config.add_section(current_section_name,
                               current_subsection_name.take(),
                               current_section);
        }

        Ok(config)
    }

    pub fn get_section(&self, section: &str, subsection: Option<&str>) -> Option<&Section> {
        let section = if section.chars().any(char::is_uppercase) {
            Cow::Owned(section.to_lowercase())
        } else {
            Cow::Borrowed(section)
        };
        let borrowed_section_key = BorrowedSectionKey(section.as_ref(), subsection);
        self.0.get(&borrowed_section_key as &dyn SectionDescriptor)
    }
}

pub struct SectionIter<'a>(hash_map::Iter<'a, SectionKey, Section>);

impl<'a> Iterator for SectionIter<'a> {
    type Item = (&'a str, Option<&'a str>, &'a Section);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| {
            (k.0.as_ref(), k.1.as_ref().map(Box::<str>::as_ref), v)
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

pub struct IntoSectionIter(hash_map::IntoIter<SectionKey, Section>);

impl Iterator for IntoSectionIter {
    type Item = (String, Option<String>, Section);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| {
            (k.0.into_string(), k.1.map(str::into_string), v)
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl IntoIterator for GitConfig {
    type Item = (String, Option<String>, Section);
    type IntoIter = IntoSectionIter;

    fn into_iter(self) -> Self::IntoIter {
        self.into_iter()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::Path;

    #[test]
    fn parse_git_config() {
        let sample = br#"# This is a test comment
[core]
autocrlf = true ; also with ;

[commit] # Should work on a section as well # ;
gpgSign = true

[remote "origin"]
path = ~/test/subdir
# and as the last line"#;
        let config = GitConfig::from_reader(sample.as_ref()).unwrap();
        let section = config.get_section("commit", None)
            .expect("Couldn't find section commit");
        assert_eq!(true, section.get_bool("gpgSign")
            .expect("Value gpgSign not existing")
            .expect("Unable to parse value gpgSign"));

        let section = config.get_section("remote", Some("origin"))
            .expect("Unable to find remote origin");
        env::set_var("HOME", "/home/example");
        assert_eq!(Path::new("/home/example/test/subdir"), section.get_path("path")
            .expect("Couldn't find path of remote"));
    }

    #[test]
    fn section_is_case_insensitive() {
        let sample = br#"[aWeIRdSEcTioN]
data = value"#;

        let config = GitConfig::from_reader(sample.as_ref()).unwrap();
        config.get_section("aweirdsection", None).unwrap();
        config.get_section("AWEIRDSECTION", None).unwrap();
        config.get_section("AWEiRdsEcTIon", None).unwrap();
    }

    #[test]
    fn missing_value_means_true() {
        let sample = br#"[section]
data"#;
        let config = GitConfig::from_reader(&sample[..]).unwrap();
        let section = config.get_section("section", None).unwrap();
        assert_eq!(true, section.get_bool("data").unwrap().unwrap());
    }

    #[test]
    fn subsection_name_escaping() {
        let sample = br#"[test "sec\\tion"]
data = value

[other "sec\"tion"]
size = 15

[other "sec\5tion"]
with = 8
"#;
        let config = GitConfig::from_reader(&sample[..]).unwrap();
        config.get_section("test", Some("sec\\tion")).unwrap();
        config.get_section("other", Some("sec\"tion")).unwrap();
        config.get_section("other", Some("sec5tion")).unwrap();
    }

    #[test]
    fn empty_section() {
        let sample = br#"[emptysection]
[nonempty]
some = data"#;

        let config = GitConfig::from_reader(&sample[..]).unwrap();
        config.get_section("emptysection", None).unwrap();
        let section = config.get_section("nonempty", None).unwrap();
        assert_eq!("data", section.get_str("some").unwrap());
    }

    #[test]
    fn section_and_subsection() {
        let sample = br#"[section]
stuff = true

[section "sub1"]
data = narf

[section "sub2"]
sata = foo"#;

        let config = GitConfig::from_reader(&sample[..]).unwrap();
        let section = config.get_section("section", None).unwrap();
        let sub1 = config.get_section("section", Some("sub1")).unwrap();
        let sub2 = config.get_section("section", Some("sub2")).unwrap();
        assert_eq!(true, section.get_bool("stuff").unwrap().unwrap());
        assert_eq!("narf", sub1.get_str("data").unwrap());
        assert_eq!("foo", sub2.get_str("sata").unwrap());
    }
}

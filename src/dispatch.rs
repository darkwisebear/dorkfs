use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::mem::replace;
use std::iter;
use std::slice;

use failure::Fallible;

#[derive(Debug)]
struct DispatcherEntry<O: Debug> {
    subpath: PathBuf,
    overlay: O
}

#[derive(Debug)]
pub struct PathDispatcher<O: Debug> {
    entries: Vec<DispatcherEntry<O>>,
    root: Option<O>
}

#[derive(Debug)]
pub struct Iter<'a, O: Debug>(
iter::Chain<
    iter::Map<slice::Iter<'a, DispatcherEntry<O>>,
        fn(&DispatcherEntry<O>) -> (&O, &Path)>,
    iter::FilterMap<iter::Once<&'a Option<O>>,
        fn(&Option<O>) -> Option<(&O, &Path)>>>);

impl<'a, O: Debug> Iterator for Iter<'a, O> {
    type Item = (&'a O, &'a Path);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl<O: Debug> PathDispatcher<O> {
    pub fn new() -> Self {
        PathDispatcher {
            entries: Vec::new(),
            root: None
        }
    }

    #[cfg(test)]
    pub fn with_root_overlay(root_overlay: O) -> Self {
        PathDispatcher {
            entries: Vec::new(),
            root: Some(root_overlay)
        }
    }

    fn resolve_path<'a, 'b>(&'a self, path: &'b Path) -> Option<(usize, &'b Path)> {
        match self.entries.binary_search_by_key(&path,
                                                |entry|
                                                    entry.subpath.as_path()) {
            Ok(exact) => Some((exact, Path::new(""))),

            Err(insert) =>
                if insert > 0 {
                    let prefix = self.entries[insert-1].subpath.as_path();
                    if let Ok(suffix) = path.strip_prefix(prefix) {
                        Some((insert-1, suffix))
                    } else {
                        None
                    }
                } else {
                    None
                }
        }
    }

    pub fn get_overlay<'a, 'b, P>(&'a self, path: &'b P) -> Fallible<Option<(&'a O, &'b Path)>>
        where P: AsRef<Path>+?Sized {
        let path = path.as_ref();
        match self.resolve_path(path) {
            Some((index, suffix)) => {
                let entry = &self.entries[index];
                Ok(Some((&entry.overlay, suffix)))
            }
            None => {
                Ok(self.root.as_ref().map(move |root| (root, path)))
            }
        }
    }

    pub fn get_overlay_mut<'a, 'b, P>(&'a mut self, path: &'b P) -> Fallible<Option<(&'a mut O, &'b Path)>>
        where P: AsRef<Path>+?Sized {
        let path = path.as_ref();
        match self.resolve_path(path) {
            Some((index, suffix)) => {
                let entry = &mut self.entries[index];
                Ok(Some((&mut entry.overlay, suffix)))
            }
            None => {
                Ok(self.root.as_mut().map(move |root| (root, path)))
            }
        }
    }

    pub fn add_overlay<P: Into<PathBuf>>(&mut self, path: P, overlay: O) -> Fallible<Option<O>> {
        let path = path.into();
        if path.as_os_str() == "/" {
            Ok(self.root.replace(overlay))
        } else {
            match self.entries.binary_search_by_key(&path.as_path(),
                                                    |entry|
                                                        entry.subpath.as_path()) {
                Ok(exact) => Ok(Some(replace(&mut self.entries[exact].overlay, overlay))),
                Err(pos) => {
                    self.entries.insert(pos, DispatcherEntry {
                        subpath: path,
                        overlay
                    });
                    Ok(None)
                }
            }
        }
    }

    fn entry_to_iter_tuple(entry: &DispatcherEntry<O>) -> (&O, &Path) {
        (&entry.overlay, entry.subpath.as_path())
    }

    fn root_overlay_to_iter_tuple(root_overlay: &Option<O>) -> Option<(&O, &Path)> {
        root_overlay.as_ref().map(|root_overlay| (root_overlay, Path::new("")))
    }

    pub fn iter(&self) -> Iter<O> {
        Iter(self.entries.iter()
            .map(Self::entry_to_iter_tuple as fn(&DispatcherEntry<O>) -> (&O, &Path))
            .chain(iter::once(&self.root)
                .filter_map(Self::root_overlay_to_iter_tuple as fn(&Option<O>) -> Option<(&O, &Path)>)))
    }
}

impl<'a, O: Debug> IntoIterator for &'a PathDispatcher<O> {
    type Item = <Iter<'a, O> as Iterator>::Item;
    type IntoIter = Iter<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn assert_submodule_paths<O: Debug>(dispatcher: &PathDispatcher<O>, ref_paths: &[&str]) {
        let paths = dispatcher.iter()
            .flat_map(|(_, path)| path.to_str().into_iter());
        assert!(paths.zip(ref_paths).all(|(lhs, rhs)| lhs == *rhs));
    }

    #[test]
    fn add_and_retrieve() {
        let mut dispatcher = PathDispatcher::with_root_overlay(0);
        assert!(dispatcher.add_overlay("somewhere/else", 1).unwrap().is_none());

        let (&root_overlay_slash, path) = dispatcher.get_overlay(Path::new(""))
            .unwrap()
            .expect("No overlay found");
        assert_eq!(root_overlay_slash, 0);
        assert_eq!(path.as_os_str(), "");

        let (&somewhere_else, path) = dispatcher.get_overlay(Path::new("somewhere/else"))
            .unwrap()
            .expect("No overlay found");
        assert_eq!(somewhere_else, 1);
        assert_eq!(path.as_os_str(), "");

        let (&totally_different, path) = dispatcher.get_overlay(Path::new("totally/different"))
            .unwrap()
            .expect("No overlay found");
        assert_eq!(totally_different, 0);
        assert_eq!(path.as_os_str(), "totally/different");

        let (&submodule_path, path) = dispatcher.get_overlay(Path::new("somewhere/else/README.md"))
            .unwrap()
            .expect("No overlay found");
        assert_eq!(path.as_os_str(), "README.md");
        assert_eq!(submodule_path, 1);

        assert_submodule_paths(&dispatcher, &["somewhere/else", ""]);
    }

    #[test]
    fn without_root_overlay() {
        let mut dispatcher = PathDispatcher::new();
        assert!(dispatcher.add_overlay("submodule/first", 0).unwrap().is_none());
        assert_eq!(dispatcher.get_overlay("anywhere").unwrap(), None);
        assert_eq!(dispatcher.get_overlay("submodule/first/dir").unwrap(),
                   Some((&0, Path::new("dir"))));

        assert_submodule_paths(&dispatcher, &["submodule/first"]);
    }
}

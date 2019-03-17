use std::ffi::{OsString, OsStr};
use std::fmt::Debug;
use std::path::{Component, Path};
use std::cell::UnsafeCell;

use failure::Fallible;

#[derive(Debug)]
struct DispatcherEntry<O: Debug> {
    name: OsString,
    overlay: Option<UnsafeCell<O>>,
    subpaths: Vec<DispatcherEntry<O>>
}

// Implementing Sync is still ok since we make sure that get_overlay[_mut] will obey to the
// Rust memory safety rules, so sending references is ok as with usual types.
unsafe impl<O: Debug+Sync> Sync for DispatcherEntry<O> {}

impl<O: Debug> DispatcherEntry<O> {
    fn find_subpath_index(&self, name: &OsStr) -> Result<usize, usize> {
        self.subpaths.binary_search_by(|entry|
            entry.name.as_os_str().cmp(name))
    }
}

#[derive(Debug)]
pub struct PathDispatcher<O: Debug> {
    root: DispatcherEntry<O>
}

impl<O: Debug> PathDispatcher<O> {
    pub fn new() -> Self {
        PathDispatcher {
            root: DispatcherEntry {
                name: OsString::new(),
                overlay: None,
                subpaths: Vec::new()
            }
        }
    }

    pub fn with_root_overlay(root_overlay: O) -> Self {
        PathDispatcher {
            root: DispatcherEntry {
                name: OsString::new(),
                overlay: Some(UnsafeCell::new(root_overlay)),
                subpaths: Vec::new()
            }
        }

    }

    unsafe fn resolve_path<'a, 'b>(&'a self, path: &'b Path) -> Fallible<Option<(&'a mut O, &'b Path)>> {
        let mut cur_overlay = self.root.overlay.as_ref()
            .map(|o| o.get());
        let mut cur_subpath = &self.root;
        let mut cur_path_suffix = path;

        for component in path.components() {
            match component {
                Component::Normal(name) => match cur_subpath.find_subpath_index(name) {
                    Ok(subpath_index) => {
                        let subpath = &cur_subpath.subpaths[subpath_index];
                        cur_path_suffix = cur_path_suffix.strip_prefix(&subpath.name).unwrap();

                        if let Some(ref suboverlay) = subpath.overlay {
                            cur_overlay = Some(suboverlay.get());
                        }
                        cur_subpath = subpath;
                    }

                    Err(_) => break,
                }

                Component::CurDir =>
                    cur_path_suffix = cur_path_suffix.strip_prefix(".").unwrap(),
                Component::RootDir =>
                    cur_path_suffix = cur_path_suffix.strip_prefix("/").unwrap(),

                Component::Prefix(_) | Component::ParentDir =>
                    bail!(r#"Path for resolve_path must not contain a prefix or "..""#)
            }
        }

        let overlay = cur_overlay.and_then(|o| o.as_mut());
        Ok(overlay.map(move |o| (o, cur_path_suffix)))
    }

    pub fn get_overlay<'a, 'b, P>(&'a self, path: &'b P) -> Fallible<Option<(&'a O, &'b Path)>>
        where P: AsRef<Path>+?Sized {
        unsafe {
            self.resolve_path(path.as_ref())
                .map(|o|
                    o.map(|(r, p)| (r as &O, p)))
        }
    }

    pub fn get_overlay_mut<'a, 'b, P>(&'a mut self, path: &'b P) -> Fallible<Option<(&'a mut O, &'b Path)>>
        where P: AsRef<Path>+?Sized {
        unsafe {
            self.resolve_path(path.as_ref())
        }
    }

    pub fn add_overlay<P: AsRef<Path>>(&mut self, path: P, overlay: O) -> Fallible<Option<O>> {
        let mut cur_subpath = &mut self.root;
        for component in path.as_ref().components() {
            match component {
                Component::Normal(name) => {
                    match cur_subpath.find_subpath_index(name) {
                        Ok(existing) => cur_subpath = &mut cur_subpath.subpaths[existing],
                        Err(insert) => {
                            let new_subpath = DispatcherEntry {
                                name: name.to_os_string(),
                                overlay: None,
                                subpaths: Vec::new()
                            };
                            cur_subpath.subpaths.insert(insert, new_subpath);
                            cur_subpath = &mut cur_subpath.subpaths[insert];
                        }
                    }
                }

                Component::RootDir | Component::CurDir => (),

                Component::Prefix(_) | Component::ParentDir =>
                    bail!(r#"Path for add_overlay must not contain a prefix or "..""#)
            }
        }

        Ok(cur_subpath.overlay.replace(UnsafeCell::new(overlay))
            .map(UnsafeCell::into_inner))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn add_and_retrieve() {
        let mut dispatcher = PathDispatcher::with_root_overlay(0);
        assert!(dispatcher.add_overlay("/somewhere/else", 1).unwrap().is_none());

        let (&root_overlay, path) = dispatcher.get_overlay("")
            .unwrap()
            .expect("No overlay found");
        assert_eq!(root_overlay, 0);
        assert_eq!(path.as_os_str(), "");

        let (&root_overlay_slash, path) = dispatcher.get_overlay(Path::new("/"))
            .unwrap()
            .expect("No overlay found");
        assert_eq!(root_overlay_slash, 0);
        assert_eq!(path.as_os_str(), "");

        let (&somewhere_else, path) = dispatcher.get_overlay(Path::new("/somewhere/else"))
            .unwrap()
            .expect("No overlay found");
        assert_eq!(somewhere_else, 1);
        assert_eq!(path.as_os_str(), "");

        let (&totally_different, path) = dispatcher.get_overlay(Path::new("/totally/different"))
            .unwrap()
            .expect("No overlay found");
        assert_eq!(totally_different, 0);
        assert_eq!(path.as_os_str(), "totally/different");

        let (&submodule_path, path) = dispatcher.get_overlay(Path::new("/somewhere/else/README.md"))
            .unwrap()
            .expect("No overlay found");
        assert_eq!(path.as_os_str(), "README.md");
        assert_eq!(submodule_path, 1);
    }

    #[test]
    fn without_root_overlay() {
        let mut dispatcher = PathDispatcher::new();
        assert!(dispatcher.add_overlay("/submodule/first", 0).unwrap().is_none());
        assert_eq!(dispatcher.get_overlay("/anywhere").unwrap(), None);
        assert_eq!(dispatcher.get_overlay("/submodule/first/dir").unwrap(), Some((&0, Path::new("dir"))));
    }
}

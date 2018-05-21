use std::ffi::{OsStr, OsString};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::borrow::{Cow, Borrow};
use failure::Error;

pub fn os_string_to_string(s: OsString) -> Result<String, Error> {
    s.into_string()
        .map_err(|s| {
            format_err!("Unable to convert {} into UTF-8", s.to_string_lossy())
        })
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct ObjectName(Arc<OsString>);

impl Borrow<OsStr> for ObjectName {
    fn borrow(&self) -> &OsStr {
        self.0.as_os_str()
    }
}

#[derive(Debug)]
pub struct OpenHandleSet<T: Debug> {
    next_handle: u64,
    open_objects: HashMap<u64, (ObjectName, T)>,
    object_names: HashMap<ObjectName, u64>
}

impl<T: Debug> OpenHandleSet<T> {
    pub fn new() -> Self {
        OpenHandleSet {
            next_handle: 0,
            open_objects: HashMap::new(),
            object_names: HashMap::new()
        }
    }

    pub fn push(&mut self, value: T, name: Cow<OsStr>) -> u64 {

        let handle = self.next_handle;
        self.next_handle += 1;

        info!("Adding {} as #{} to open objects storage", name.to_string_lossy(), handle);

        let identifier = ObjectName(Arc::new(name.into_owned()));
        self.open_objects.insert(handle, (identifier.clone(), value));
        self.object_names.insert(identifier, handle);

        handle
    }

    pub fn get(&self, handle: u64) -> Option<&T> {
        self.open_objects.get(&handle)
            .map(|(_, ref obj) | obj)
    }

    pub fn get_mut(&mut self, handle: u64) -> Option<&mut T> {
        self.open_objects.get_mut(&handle)
            .map(|(_, ref mut obj)| obj)
    }

    pub fn get_named<S: AsRef<OsStr>>(&self, name: S) -> Option<&T> {
        self.object_names.get(name.as_ref())
            .and_then(|handle| self.get(*handle))
    }

    pub fn get_named_mut<S: AsRef<OsStr>>(&mut self, name: S) -> Option<&mut T> {
        match self.object_names.get(name.as_ref()).cloned() {
            Some(handle) => self.get_mut(handle),
            None => None
        }
    }

    pub fn remove(&mut self, handle: u64) -> Option<T> {
        self.open_objects.remove(&handle)
            .map(|(name, obj)| {
                self.object_names.remove(name.0.as_os_str());
                obj
            })
    }
}

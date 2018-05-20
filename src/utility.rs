use std::ffi::OsString;
use failure::Error;

pub fn os_string_to_string(s: OsString) -> Result<String, Error> {
    s.into_string()
        .map_err(|s| {
            format_err!("Unable to convert {} into UTF-8", s.to_string_lossy())
        })
}

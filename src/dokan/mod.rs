use std::{
    ffi::OsStr,
    path::{self, Path, PathBuf},
    sync::Mutex,
    io::{Read, Write, Seek, SeekFrom},
    hash::{Hash, Hasher},
    collections::hash_map::DefaultHasher,
    time::SystemTime,
    mem::MaybeUninit,
};

use dokan::{
    FileSystemHandler, Drive, OperationInfo, CreateFileInfo, OperationError,
    DOKAN_IO_SECURITY_CONTEXT, FileInfo, VolumeInfo, DiskSpaceInfo, FindData, FillDataError};
use log::*;
use widestring::{U16CString, U16CStr, WideCString, WideCStr};
use winapi::{
    um::winnt,
    shared::ntstatus,
    shared::minwindef
};
use tokio;
use futures::future;
use failure::{self, Fallible, format_err};

use crate::{
    overlay::{self, Overlay, OverlayDirEntry},
    types
};

struct RefVolInfoData {
    volume_info: VolumeInfo,
    disk_space_info: DiskSpaceInfo
}

struct RefVolInfo {
    path: WideCString,
    info: Option<RefVolInfoData>
}

impl RefVolInfo {
    fn new(path: WideCString) -> Self {
        Self {
            path,
            info: None
        }
    }

    fn query_data(&mut self) -> Result<&RefVolInfoData, OperationError> {
        if self.info.is_none() {
            const OUTBUF_LEN: usize = 256;

            let (vol_name, fs_name, vol_serial, max_component_length, fs_flags) = unsafe {
                let mut vol_name = [MaybeUninit::<winnt::WCHAR>::uninit(); OUTBUF_LEN];
                let mut fs_name = [MaybeUninit::<winnt::WCHAR>::uninit(); OUTBUF_LEN];
                let mut vol_serial = MaybeUninit::<minwindef::DWORD>::uninit();
                let mut max_component_len = MaybeUninit::<minwindef::DWORD>::uninit();
                let mut filesystem_flags = MaybeUninit::<minwindef::DWORD>::uninit();
                use std::mem::transmute;
                if winapi::um::fileapi::GetVolumeInformationW(
                    self.path.as_ptr(),
                    vol_name.as_mut_ptr() as *mut u16,
                    vol_name.len() as minwindef::DWORD,
                    vol_serial.as_mut_ptr(),
                    max_component_len.as_mut_ptr(),
                    filesystem_flags.as_mut_ptr(),
                    fs_name.as_mut_ptr() as *mut u16,
                    fs_name.len() as minwindef::DWORD) != 0 {
                    Ok((transmute::<_, [winnt::WCHAR; OUTBUF_LEN]>(vol_name),
                        transmute::<_, [winnt::WCHAR; OUTBUF_LEN]>(fs_name),
                        vol_serial.assume_init(),
                        max_component_len.assume_init(),
                        filesystem_flags.assume_init()))
                } else {
                    let err = winapi::um::errhandlingapi::GetLastError();
                    warn!("Retrieving volume information for {} caused an error: {}",
                          self.path.to_string_lossy(), err);
                    Err(OperationError::Win32(err))
                }
            }?;

            let vol_name = Self::ucstr_from_wchar_slice(vol_name.as_ref())?;
            let fs_name = Self::ucstr_from_wchar_slice(fs_name.as_ref())?;

            debug!("GetVolumeInformation of {} -> fs_name: {} serial_number: {} name: {} max_component_length: {}",
                   self.path.to_string_lossy(), fs_name.to_string_lossy(), vol_serial, vol_name.to_string_lossy(), max_component_length);

            let volume_info = VolumeInfo {
                fs_name: fs_name.to_owned(),
                serial_number: vol_serial + 1,
                name: vol_name.to_owned(),
                fs_flags,
                max_component_length
            };

            let disk_space_info = unsafe {
                let mut byte_count = MaybeUninit::<winnt::ULARGE_INTEGER>::uninit();
                let mut free_byte_count = MaybeUninit::<winnt::ULARGE_INTEGER>::uninit();
                let mut available_byte_count = MaybeUninit::<winnt::ULARGE_INTEGER>::uninit();
                if winapi::um::fileapi::GetDiskFreeSpaceExW(
                    self.path.as_ptr(),
                    available_byte_count.as_mut_ptr(),
                    byte_count.as_mut_ptr(),
                    free_byte_count.as_mut_ptr()) != 0 {
                    Ok(DiskSpaceInfo {
                        available_byte_count: *available_byte_count.assume_init().QuadPart(),
                        byte_count: *byte_count.assume_init().QuadPart(),
                        free_byte_count: *free_byte_count.assume_init().QuadPart()

                    })
                } else {
                    Err(OperationError::Win32(winapi::um::errhandlingapi::GetErrorMode()))
                }
            }?;

            self.info = Some(RefVolInfoData {
                volume_info,
                disk_space_info
            });
        }

        Ok(self.info.as_ref().unwrap())
    }

    fn ucstr_from_wchar_slice(vol_name: &[u16]) -> Result<&U16CStr, OperationError> {
        WideCStr::from_slice_with_nul(vol_name)
            .map_err(|e| {
                error!("Bad volume name: {}", e);
                OperationError::NtStatus(ntstatus::STATUS_BAD_DATA)
            })
    }
}

struct State<O> {
    root_overlay: O,
    ref_vol_info: RefVolInfo
}

pub struct DorkFs<O> {
    state: Mutex<State<O>>,
    tokio_runtime: tokio::runtime::Handle,
    vol_name: U16CString
}

impl<O> DorkFs<O> where O: Send+Sync+Overlay, <O as Overlay>::File: Send {
    pub fn with_overlay(root_overlay: O, ref_volume: &Path, volume_name: impl AsRef<str>) -> Fallible<Self> {
        let mut prefix = ref_volume.components()
            .find(|c| if let path::Component::Prefix(_) = c { true } else { false })
            .ok_or_else(|| format_err!("Unable to extract drive from mount point {}",
                                           ref_volume.to_string_lossy()))?
            .as_os_str()
            .to_owned();

        let vol_name = U16CString::from_str(volume_name)?;

        prefix.push("\\");
        info!("Using {} as reference volume", prefix.to_string_lossy());

        let state = State {
            root_overlay,
            ref_vol_info: RefVolInfo::new(WideCString::from_os_str(&prefix)?)
        };

        Ok(DorkFs {
            state: Mutex::new(state),
            vol_name,
            tokio_runtime: tokio::runtime::Handle::current()
        })
    }

    pub fn mount(self, mountpoint: impl AsRef<OsStr>) {
        let mut drive = Drive::new();
        drive
            .mount_point(&U16CString::from_os_str(mountpoint).unwrap())
            .mount(&self).unwrap()
    }

    fn make_overlay_path(path: &U16CStr) -> PathBuf {
        let slice = path.as_slice_with_nul();
        let stripped = unsafe {
            assert!(slice.len() >= 2);
            U16CStr::from_slice_with_nul_unchecked(&slice[1..])
        };
        PathBuf::from(stripped.to_os_string())
    }

    fn do_create_file(&self, file_name: &U16CStr)
        -> Result<CreateFileInfo<<Self as FileSystemHandler>::Context>, OperationError> {
        let path = Self::make_overlay_path(file_name);
        debug!("create_file {}", path.display());
        let mut state = self.state.lock().unwrap();
        let root_overlay = &mut state.root_overlay;
        let (is_dir, new_file_created) = match root_overlay.metadata(&path) {
            Ok(metadata) => (metadata.object_type == types::ObjectType::Directory, false),
            Err(overlay::Error::FileNotFound) => (false, true),
            Err(e) => {
                warn!("Unable to get metadata in create_file: {}", e);
                return Err(OperationError::NtStatus(ntstatus::STATUS_FILE_INVALID));
            }
        };

        let context = if !is_dir {
            let file = root_overlay.open_file(&path, false)
                .map_err(|e| match e {
                    overlay::Error::FileNotFound => OperationError::NtStatus(ntstatus::STATUS_FILE_NOT_AVAILABLE),
                    _ => {
                        warn!("Unable to open file: {}", e);
                        OperationError::NtStatus(ntstatus::STATUS_FILE_CORRUPT_ERROR)
                    }
                })?;
            Some(Mutex::new(file))
        } else {
            None
        };

        Ok(CreateFileInfo {
            context,
            is_dir,
            new_file_created
        })
    }

    fn do_find_files(&self,
                     file_name: &U16CStr,
                     mut fill_find_data: impl FnMut(&FindData) -> Result<(), FillDataError>)
        -> Result<(), OperationError> {
        let path = Self::make_overlay_path(file_name);
        debug!("find_files in {}", path.display());
        let mut state = self.state.lock().unwrap();
        let root_overlay = &mut state.root_overlay;
        for find_data in root_overlay.list_directory(&path)
            .map_err(|e| {
                warn!("Unable to list directory: {}", e);
                OperationError::NtStatus(ntstatus::STATUS_DIRECTORY_NOT_SUPPORTED)
            })?
            .map(|entry| {
                entry
                    .map_err(|e| {
                        warn!("Unable to retrieve an entry: {}", e);
                        OperationError::NtStatus(ntstatus::STATUS_DIRECTORY_NOT_SUPPORTED)
                    })
                    .and_then(Self::dir_entry_to_find_data)
            }) {
            if let Err(e) = fill_find_data(&find_data?) {
                warn!("Unable to deliver directory entry data: {}", e);
                return Err(OperationError::NtStatus(ntstatus::STATUS_DIRECTORY_NOT_SUPPORTED));
            }
        }

        Ok(())
    }

    fn attributes_from_object_type(obj_type: types::ObjectType) -> u32 {
        match obj_type {
            types::ObjectType::Directory => winnt::FILE_ATTRIBUTE_DIRECTORY,
            types::ObjectType::File | types::ObjectType::Pipe =>
                winnt::FILE_ATTRIBUTE_NORMAL,
            types::ObjectType::Symlink => winnt::FILE_ATTRIBUTE_REPARSE_POINT,
        }
    }

    fn dir_entry_to_find_data(entry: OverlayDirEntry) -> Result<FindData, OperationError> {
        let file_name = U16CString::from_str(&entry.name)
            .map_err(|e| {
                warn!("File name {} contains a null character at {}",
                      entry.name, e.nul_position());
                OperationError::NtStatus(ntstatus::STATUS_DIRECTORY_NOT_SUPPORTED)
            })?;
        let modified_date = SystemTime::from(entry.modified_date);

        Ok(FindData {
            attributes: Self::attributes_from_object_type(entry.object_type),
            creation_time: modified_date,
            last_access_time: modified_date,
            last_write_time: modified_date,
            file_size: entry.size,
            file_name,
        })
    }
}

impl<'a, 'b, O> dokan::FileSystemHandler<'a, 'b> for DorkFs<O>
    where 'b: 'a,
          O: Send+Sync+Overlay+'b,
          <O as Overlay>::File: Send {
    type Context = Option<Mutex<<O as Overlay>::File>>;

    fn create_file(&'b self,
                   file_name: &U16CStr,
                   _security_context: *mut DOKAN_IO_SECURITY_CONTEXT,
                   _desired_access: u32,
                   _file_attributes: u32,
                   _share_access: u32,
                   _create_disposition: u32,
                   _create_options: u32,
                   _info: &mut OperationInfo<'a, 'b, Self>)
        -> Result<CreateFileInfo<Self::Context>, OperationError> {
        self.tokio_runtime.block_on(future::lazy(|_| self.do_create_file(file_name)))
    }

    fn close_file(&'b self,
                  _file_name: &U16CStr,
                  _info: &OperationInfo<'a, 'b, Self>,
                  _context: &'a Self::Context) {
    }

    fn read_file(&'b self, _file_name: &U16CStr, offset: i64, buffer: &mut [u8],
                 _info: &OperationInfo<'a, 'b, Self>, context: &'a Self::Context)
        -> Result<u32, OperationError> {
        self.tokio_runtime.block_on(future::lazy(|_| {
            let mut file = context.as_ref().unwrap().lock().unwrap();
            file.seek(SeekFrom::Start(offset as u64))
                .map_err(|e| {
                    warn!("Unable to seek before read: {}", e);
                    OperationError::NtStatus(ntstatus::STATUS_FILE_INVALID)
                })?;
            file.read(buffer)
                .map(|num_bytes| num_bytes as u32)
                .map_err(|e| {
                    warn!("Error during file reading: {}", e);
                    OperationError::NtStatus(ntstatus::STATUS_FILE_INVALID)
                })
        }))
    }

    fn write_file(&'b self, _file_name: &U16CStr, offset: i64, buffer: &[u8],
                  _info: &OperationInfo<'a, 'b, Self>, context: &'a Self::Context)
        -> Result<u32, OperationError> {
        self.tokio_runtime.block_on(future::lazy(|_| {
            let mut file = context.as_ref().unwrap().lock().unwrap();
            file.seek(SeekFrom::Start(offset as u64))
                .map_err(|e| {
                    warn!("Unable to seek before write: {}", e);
                    OperationError::NtStatus(ntstatus::STATUS_FILE_INVALID)
                })?;
            file.write(buffer)
                .map(|num_bytes| num_bytes as u32)
                .map_err(|e| {
                    warn!("Error during file writing: {}", e);
                    OperationError::NtStatus(ntstatus::STATUS_FILE_INVALID)
                })
        }))
    }

    fn get_file_information(&'b self, file_name: &U16CStr, _info: &OperationInfo<'a, 'b, Self>,
                            _context: &'a Self::Context) -> Result<FileInfo, OperationError> {
        let path = Self::make_overlay_path(file_name);
        debug!("get_file_information on {}", path.display());

        let metadata = self.tokio_runtime.block_on(future::lazy(|_| {
            let state = self.state.lock().unwrap();
            state.root_overlay.metadata(&path)
        }))
        .map_err(|e| {
            warn!("Unable to retrieve file info for {}: {}", path.display(), e);
            OperationError::NtStatus(ntstatus::STATUS_FILE_NOT_AVAILABLE)
        })?;

        let modified_date = SystemTime::from(metadata.modified_date);

        let mut hasher = DefaultHasher::new();
        file_name.hash(&mut hasher);
        let file_index = hasher.finish();

        Ok(FileInfo {
            creation_time: modified_date,
            last_access_time: modified_date,
            last_write_time: modified_date,
            file_size: metadata.size,
            attributes: Self::attributes_from_object_type(metadata.object_type),
            number_of_links: 1,
            file_index
        })
    }

    fn find_files(&'b self,
                  file_name: &U16CStr,
                  fill_find_data: impl FnMut(&FindData) -> Result<(), FillDataError>,
                  _info: &OperationInfo<'a, 'b, Self>,
                  _context: &'a Self::Context)
        -> Result<(), OperationError> {
        self.tokio_runtime.block_on(
            future::lazy(|_| self.do_find_files(file_name, fill_find_data)))
    }

    fn find_files_with_pattern(&'b self,
                               file_name: &U16CStr,
                               pattern: &U16CStr,
                               _fill_find_data: impl FnMut(&FindData) -> Result<(), FillDataError>,
                               _info: &OperationInfo<'a, 'b, Self>,
                               _context: &'a Self::Context)
        -> Result<(), OperationError> {
        debug!("find_files_with_pattern file_name: {} pattern: {}",
               file_name.to_string_lossy(), pattern.to_string_lossy());
        Err(OperationError::NtStatus(ntstatus::STATUS_NOT_IMPLEMENTED))
    }

    fn get_disk_free_space(&'b self,
                           _info: &OperationInfo<'a, 'b, Self>)
        -> Result<DiskSpaceInfo, OperationError> {
        self.state.lock().unwrap().ref_vol_info
            .query_data()
            .map(|info| info.disk_space_info.clone())
    }

    fn get_volume_information(&'b self, _info: &OperationInfo<'a, 'b, Self>)
        -> Result<VolumeInfo, OperationError> {
        self.state.lock().unwrap().ref_vol_info
            .query_data()
            .map(|info| {
                let mut vol_info = info.volume_info.clone();
                vol_info.name = self.vol_name.clone();
                vol_info
            })
    }
}

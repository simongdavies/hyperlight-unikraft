// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{Error, Result};
use std::io::Read;
use std::path::Path;

/// Read only an operator-admitted fixed mapping. No guest name/path lookup,
/// interpolation or secret value is included in errors.
pub(super) fn read_private_secret(path: &Path) -> Result<Vec<u8>> {
    read_private_file(path, false)
}

pub(super) fn read_expiring_header_file(path: &Path) -> Result<Vec<u8>> {
    read_private_file(path, true)
}

fn read_private_file(path: &Path, expiring: bool) -> Result<Vec<u8>> {
    let limit = if expiring { 8192 } else { 4096 };
    if !path.is_absolute() {
        return Err(unavailable());
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|_| unavailable())?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(unavailable());
    }
    #[cfg(unix)]
    let file = {
        let mut flags =
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC;
        if expiring {
            flags |= rustix::fs::OFlags::NONBLOCK;
        }
        let descriptor =
            rustix::fs::open(path, flags, rustix::fs::Mode::empty()).map_err(|_| unavailable())?;
        std::fs::File::from(descriptor)
    };
    #[cfg(not(unix))]
    let file = std::fs::File::open(path).map_err(|_| unavailable())?;
    let metadata = file.metadata().map_err(|_| unavailable())?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > limit {
        return Err(unavailable());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(unavailable());
        }
        if expiring {
            use std::os::unix::fs::MetadataExt;
            if !expiring_access_allowed(
                metadata.permissions().mode(),
                metadata.uid(),
                rustix::process::geteuid().as_raw(),
            ) {
                return Err(unavailable());
            }
        }
    }
    #[cfg(not(unix))]
    if expiring {
        return Err(unavailable());
    }
    let mut bytes = Vec::new();
    if file.take(limit + 1).read_to_end(&mut bytes).is_err() {
        bytes.fill(0);
        return Err(unavailable());
    }
    if bytes.is_empty() || bytes.len() as u64 > limit {
        bytes.fill(0);
        return Err(unavailable());
    }
    Ok(bytes)
}

#[cfg(unix)]
fn expiring_access_allowed(mode: u32, owner: u32, current: u32) -> bool {
    mode & 0o7777 == 0o600 && owner == current
}

fn unavailable() -> Error {
    Error::State("host secret reference unavailable or invalid".into())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn expiring_files_require_the_same_owner_and_exact_private_mode() {
        assert!(expiring_access_allowed(0o100600, 1000, 1000));
        for (mode, owner) in [
            (0o100600, 1001),
            (0o100640, 1000),
            (0o100400, 1000),
            (0o104600, 1000),
        ] {
            assert!(!expiring_access_allowed(mode, owner, 1000));
        }
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("source");
        std::fs::write(&file, b"{}").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(read_expiring_header_file(&file).unwrap(), b"{}");
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(read_expiring_header_file(&file).is_err());
    }

    #[test]
    fn expiring_reader_rejects_links_and_oversize_without_changing_raw_limit() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("source");
        std::fs::write(&file, vec![b'x'; 8192]).unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(read_expiring_header_file(&file).unwrap().len(), 8192);
        assert!(read_private_secret(&file).is_err());
        std::fs::write(&file, vec![b'x'; 8193]).unwrap();
        assert!(read_expiring_header_file(&file).is_err());
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(&file, &link).unwrap();
        assert!(read_expiring_header_file(&link).is_err());
        assert!(read_expiring_header_file(std::path::Path::new("relative")).is_err());
    }
}

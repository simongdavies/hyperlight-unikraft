// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{Error, Result};
use std::io::Read;
use std::path::Path;

/// Read only an operator-admitted fixed mapping. No guest name/path lookup,
/// interpolation or secret value is included in errors.
pub(super) fn read_private_secret(path: &Path) -> Result<Vec<u8>> {
    if !path.is_absolute() {
        return Err(unavailable());
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|_| unavailable())?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(unavailable());
    }
    #[cfg(unix)]
    let file = {
        let descriptor = rustix::fs::open(
            path,
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|_| unavailable())?;
        std::fs::File::from(descriptor)
    };
    #[cfg(not(unix))]
    let file = std::fs::File::open(path).map_err(|_| unavailable())?;
    let metadata = file.metadata().map_err(|_| unavailable())?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > 4096 {
        return Err(unavailable());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(unavailable());
        }
    }
    let mut bytes = Vec::new();
    file.take(4097)
        .read_to_end(&mut bytes)
        .map_err(|_| unavailable())?;
    if bytes.is_empty() || bytes.len() > 4096 {
        bytes.fill(0);
        return Err(unavailable());
    }
    Ok(bytes)
}

fn unavailable() -> Error {
    Error::State("host secret reference unavailable or invalid".into())
}

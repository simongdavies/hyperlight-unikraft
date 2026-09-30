// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{Error, PROTOCOL_VERSION, Result, WorkerBundle, WorkerVersionId};
use crate::Snapshot;
use hyperlight_host::sandbox::snapshot::OciDigest;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

const SCHEMA_VERSION: u16 = 4;
const METADATA_FILE: &str = "worker.json";
const CAPABILITIES: &[u8] = b"stdout:ndjson-response:v1;console:bounded;stdin:denied;\
hostfs:none;hostsock:none;fetch:hcall:v1,v2;timer:hcall:v1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotBinding {
    worker_version: WorkerVersionId,
    bundle_sha256: String,
    kernel_sha256: String,
    rootfs_sha256: String,
    executor_sha256: String,
    dependency_closure_sha256: String,
    capability_set_sha256: String,
}

impl SnapshotBinding {
    /// The guest image and executor must come from the same trusted build.
    /// This records hashes, not evidence that an arbitrary CPIO embeds that executor.
    pub fn from_artifacts(
        bundle: &WorkerBundle,
        rootfs: impl AsRef<Path>,
        executor: impl AsRef<Path>,
    ) -> Result<Self> {
        bundle.validate()?;
        let kernel = kernel_for_rootfs(rootfs.as_ref())?;
        Ok(Self {
            worker_version: bundle.worker_version.clone(),
            bundle_sha256: bundle.sha256()?,
            kernel_sha256: sha256(kernel),
            rootfs_sha256: sha256_file(rootfs.as_ref())?,
            executor_sha256: sha256_file(executor.as_ref())?,
            dependency_closure_sha256: dependency_closure_sha256(executor.as_ref())?,
            capability_set_sha256: sha256(CAPABILITIES),
        })
    }

    pub fn worker_version(&self) -> &WorkerVersionId {
        &self.worker_version
    }

    pub fn bundle_sha256(&self) -> &str {
        &self.bundle_sha256
    }

    fn validate(&self) -> Result<()> {
        for digest in [
            &self.kernel_sha256,
            &self.bundle_sha256,
            &self.rootfs_sha256,
            &self.executor_sha256,
            &self.dependency_closure_sha256,
            &self.capability_set_sha256,
        ] {
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(Error::Snapshot("invalid SHA-256 digest".into()));
            }
        }
        if self.kernel_sha256 != sha256(crate::KERNEL)
            && self.kernel_sha256 != sha256(crate::WORKERD_KERNEL)
        {
            return Err(Error::Snapshot("kernel or capability set mismatch".into()));
        }
        if self.capability_set_sha256 != sha256(CAPABILITIES) {
            return Err(Error::Snapshot("kernel or capability set mismatch".into()));
        }
        Ok(())
    }
}

pub(super) fn kernel_for_rootfs(rootfs: &Path) -> Result<&'static [u8]> {
    let mut magic = [0; 4];
    File::open(rootfs)?.read_exact(&mut magic)?;
    Ok(if magic == *b"\x7fELF" {
        crate::WORKERD_KERNEL
    } else {
        crate::KERNEL
    })
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    schema_version: u16,
    protocol_version: u16,
    host_version: String,
    manifest_digest: String,
    binding: SnapshotBinding,
}

impl Metadata {
    fn validate(&self, expected: &SnapshotBinding) -> Result<OciDigest> {
        self.binding.validate()?;
        expected.validate()?;
        if self.schema_version != SCHEMA_VERSION
            || self.protocol_version != PROTOCOL_VERSION
            || self.host_version != env!("CARGO_PKG_VERSION")
            || &self.binding != expected
        {
            return Err(Error::Snapshot(
                "snapshot schema, protocol, host or Worker artifact binding mismatch".into(),
            ));
        }
        self.manifest_digest
            .parse()
            .map_err(|e| Error::Snapshot(format!("invalid OCI digest: {e}")))
    }
}

/// An initialized, version- and canonical-bundle-bound snapshot.
///
/// Persisted layouts must be trusted and remain immutable for the lifetime
/// of this value and every sandbox using it. Hyperlight memory-maps the OCI
/// layer. Read-only permissions prevent accidents, not a hostile file owner.
/// Digests provide integrity, not authenticity.
#[derive(Clone)]
pub struct VerifiedSnapshot {
    pub(super) snapshot: Arc<Snapshot>,
    binding: SnapshotBinding,
}

impl VerifiedSnapshot {
    pub(super) fn initialized(snapshot: Arc<Snapshot>, binding: SnapshotBinding) -> Self {
        Self { snapshot, binding }
    }

    pub fn binding(&self) -> &SnapshotBinding {
        &self.binding
    }

    /// Write a fresh OCI directory. Never overwrite an existing version.
    /// On failure the partial directory is left for explicit operator cleanup.
    pub fn save(&self, directory: impl AsRef<Path>) -> Result<()> {
        let directory = directory.as_ref();
        fs::create_dir(directory)?;
        let digest = self.snapshot.save(directory, &crate::snapshot_tag())?;
        let metadata = Metadata {
            schema_version: SCHEMA_VERSION,
            protocol_version: PROTOCOL_VERSION,
            host_version: env!("CARGO_PKG_VERSION").into(),
            manifest_digest: digest.to_string(),
            binding: self.binding.clone(),
        };
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join(METADATA_FILE))?;
        file.write_all(&serde_json::to_vec_pretty(&metadata)?)?;
        file.sync_all()?;
        seal_or_check(directory, true)?;
        Ok(())
    }

    /// Verify the *entire* expected binding supplied by the trusted deployer,
    /// then SHA-256-check the manifest, config and mapped snapshot layer.
    pub fn open(directory: impl AsRef<Path>, expected: &SnapshotBinding) -> Result<Self> {
        let directory = directory.as_ref();
        seal_or_check(directory, false)?;
        let mut bytes = Vec::new();
        File::open(directory.join(METADATA_FILE))?
            .take(16 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 16 * 1024 {
            return Err(Error::Snapshot(
                "snapshot metadata exceeds size limit".into(),
            ));
        }
        let metadata: Metadata = serde_json::from_slice(&bytes)?;
        let digest = metadata.validate(expected)?;
        let snapshot = Arc::new(Snapshot::checked_load(directory, digest)?);
        Ok(Self {
            snapshot,
            binding: metadata.binding,
        })
    }
}

fn seal_or_check(path: &Path, seal: bool) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(Error::Snapshot("snapshot layout contains a symlink".into()));
    }
    if metadata.is_dir() {
        for entry in fs::read_dir(path)? {
            seal_or_check(&entry?.path(), seal)?;
        }
    } else if !metadata.is_file() {
        return Err(Error::Snapshot(
            "snapshot layout contains a non-regular file".into(),
        ));
    }
    if seal {
        let mut permissions = metadata.permissions();
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions)?;
    } else if !metadata.permissions().readonly() {
        return Err(Error::Snapshot(format!(
            "snapshot path is not read-only: {}",
            path.display()
        )));
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hex(&hash.finalize()))
}

fn dependency_closure_sha256(executor: &Path) -> Result<String> {
    let Some(directory) = executor.parent() else {
        return Ok(sha256(b"no packaged dependency manifest"));
    };
    let path = directory.join("dependency-closure.sha256");
    if !path.exists() {
        return Ok(sha256(b"no packaged dependency manifest"));
    }
    let mut digest = String::new();
    File::open(path)?.take(66).read_to_string(&mut digest)?;
    let digest = digest.trim();
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::Snapshot(
            "invalid packaged dependency closure digest".into(),
        ));
    }
    Ok(digest.into())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut result, "{byte:02x}").expect("writing to String");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writable_layout_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(seal_or_check(tmp.path(), false).is_err());
        // TempDir owns cleanup; restore permissions after checking directory sealing.
        let original = fs::metadata(tmp.path()).unwrap().permissions();
        seal_or_check(tmp.path(), true).unwrap();
        seal_or_check(tmp.path(), false).unwrap();
        fs::set_permissions(tmp.path(), original).unwrap();
    }

    #[test]
    fn sha256_known_vector_and_all_binding_fields() {
        assert_eq!(
            sha256(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let tmp = tempfile::tempdir().unwrap();
        let rootfs = tmp.path().join("rootfs");
        let executor = tmp.path().join("executor");
        fs::write(&rootfs, b"rootfs").unwrap();
        fs::write(&executor, b"executor").unwrap();
        fs::write(
            tmp.path().join("dependency-closure.sha256"),
            format!("{}\n", "a".repeat(64)),
        )
        .unwrap();
        let binding = SnapshotBinding::from_artifacts(&bundle(), rootfs, executor).unwrap();
        let mut metadata = Metadata {
            schema_version: SCHEMA_VERSION,
            protocol_version: PROTOCOL_VERSION,
            host_version: env!("CARGO_PKG_VERSION").into(),
            manifest_digest: format!("sha256:{}", "1".repeat(64)),
            binding: binding.clone(),
        };
        metadata.validate(&binding).unwrap();
        for field in [
            "worker_version",
            "bundle_sha256",
            "kernel_sha256",
            "rootfs_sha256",
            "executor_sha256",
            "dependency_closure_sha256",
            "capability_set_sha256",
        ] {
            let mut value = serde_json::to_value(&binding).unwrap();
            value[field] = if field == "worker_version" {
                "v2".into()
            } else {
                "0".repeat(64).into()
            };
            let other = serde_json::from_value(value).unwrap();
            assert!(metadata.validate(&other).is_err(), "{field}");
        }
        metadata.schema_version += 1;
        assert!(metadata.validate(&binding).is_err());
        metadata.schema_version = SCHEMA_VERSION;
        metadata.protocol_version += 1;
        assert!(metadata.validate(&binding).is_err());
        metadata.protocol_version = PROTOCOL_VERSION;
        metadata.host_version = "old".into();
        assert!(metadata.validate(&binding).is_err());
    }

    fn bundle() -> WorkerBundle {
        WorkerBundle::single_script(
            WorkerVersionId::new("v1").unwrap(),
            "2025-01-01",
            "worker.js",
            "export default { fetch() { return new Response('ok') } }",
        )
        .unwrap()
    }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Authenticated instance checkpoints on a persistent local filesystem.
//! SQLite transactions provide crash-safe atomic publication and ownership.
//! This is not a distributed database: do not place it on NFS or claim
//! cross-node recovery without qualifying the persistent-volume backend.

use super::{Error, Result, SnapshotBinding, VerifiedSnapshot};
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path};
use std::sync::{Arc, Mutex};

const CHUNK_BYTES: usize = 1024 * 1024;
const MAX_FILES: usize = 64;

fn crypto_error() -> Error {
    Error::Snapshot("checkpoint authentication or encryption failed".into())
}

pub(super) fn random_id() -> Result<String> {
    let mut bytes = [0; 16];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| crypto_error())?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointPolicy {
    #[default]
    Durable,
    Local,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceIdentity {
    pub instance_id: String,
    pub generation: u64,
}

#[derive(Clone, Debug)]
pub struct CheckpointRecord {
    pub identity: InstanceIdentity,
    pub state: super::InstanceState,
    pub checkpoint_id: Option<String>,
    pub requests_served: u64,
}

impl InstanceIdentity {
    pub fn new() -> Result<Self> {
        Ok(Self {
            instance_id: random_id()?,
            generation: 1,
        })
    }
}

/// Holds the decrypted private layout alive while Hyperlight memory-maps it.
/// Only one claim can be acquired for a parked generation.
pub struct CheckpointClaim {
    pub identity: InstanceIdentity,
    pub checkpoint_id: String,
    owner: String,
    image: VerifiedSnapshot,
    layout: Arc<CheckpointLayout>,
}

impl CheckpointClaim {
    pub fn snapshot(&self) -> &VerifiedSnapshot {
        &self.image
    }
    pub(super) fn layout(&self) -> Arc<CheckpointLayout> {
        self.layout.clone()
    }
}

pub(super) struct CheckpointLayout(Option<tempfile::TempDir>);

impl CheckpointLayout {
    fn new() -> Result<Self> {
        Ok(Self(Some(
            tempfile::Builder::new()
                .prefix("hyperloom-checkpoint-")
                .tempdir()?,
        )))
    }
    fn new_in(directory: Option<&Path>) -> Result<Self> {
        match directory {
            Some(directory) => Ok(Self(Some(
                tempfile::Builder::new()
                    .prefix("hyperloom-checkpoint-")
                    .tempdir_in(directory)?,
            ))),
            None => Self::new(),
        }
    }
    fn path(&self) -> &Path {
        self.0
            .as_ref()
            .expect("checkpoint layout has not been closed")
            .path()
    }
}

impl Drop for CheckpointLayout {
    fn drop(&mut self) {
        if let Some(directory) = self.0.take() {
            // Verified OCI layouts are sealed read-only, including their
            // directories. Unseal our own private scratch before deleting it.
            if let Err(error) = unseal_private(directory.path()) {
                tracing::error!(path = %directory.path().display(), %error, "checkpoint plaintext cleanup unseal failed");
            }
            if let Err(error) = directory.close() {
                tracing::error!(%error, "checkpoint plaintext cleanup failed");
            }
        }
    }
}

pub struct CheckpointStore {
    db: Mutex<Connection>,
    key: LessSafeKey,
    max_bytes: u64,
    scratch_directory: Option<std::path::PathBuf>,
}

impl std::fmt::Debug for CheckpointStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CheckpointStore")
            .field("max_bytes", &self.max_bytes)
            .finish_non_exhaustive()
    }
}

impl CheckpointStore {
    pub fn records(&self, binding: &SnapshotBinding) -> Result<Vec<CheckpointRecord>> {
        let db = self.database()?;
        let binding_json = serde_json::to_string(binding)?;
        let mut statement=db.prepare("SELECT id,generation,state,checkpoint_id,checkpoint_generation FROM instances WHERE binding=?1 AND state!='released' ORDER BY id")
            .map_err(db_error)?;
        let mut rows = statement.query([&binding_json]).map_err(db_error)?;
        let mut records = Vec::new();
        while let Some(row) = rows.next().map_err(db_error)? {
            let state: String = row.get(2).map_err(db_error)?;
            let state = match state.as_str() {
                "active" => super::InstanceState::Active,
                "checkpointing" => super::InstanceState::Checkpointing,
                "parked" => super::InstanceState::Parked,
                "resuming" => super::InstanceState::Resuming,
                "failed" => super::InstanceState::Failed,
                _ => {
                    return Err(Error::Snapshot(
                        "unknown persisted instance lifecycle state".into(),
                    ));
                }
            };
            let identity = InstanceIdentity {
                instance_id: row.get(0).map_err(db_error)?,
                generation: row.get(1).map_err(db_error)?,
            };
            let checkpoint_id: Option<String> = row.get(3).map_err(db_error)?;
            let checkpoint_generation: Option<u64> = row.get(4).map_err(db_error)?;
            let requests_served = match (&checkpoint_id, checkpoint_generation) {
                (Some(checkpoint), Some(generation)) => {
                    let captured = InstanceIdentity {
                        instance_id: identity.instance_id.clone(),
                        generation,
                    };
                    validate_identity(&captured)?;
                    let (parts, bytes): (u64, Option<Vec<u8>>) = db
                        .query_row(
                            "SELECT COUNT(*),
                             (SELECT ciphertext FROM checkpoint_chunks WHERE checkpoint_id=?1 AND instance_id=?2
                              AND path=?3 AND part=0 AND length(ciphertext)<=?4)
                             FROM checkpoint_chunks WHERE checkpoint_id=?1 AND instance_id=?2 AND path=?3",
                            params![
                                checkpoint,
                                identity.instance_id,
                                super::snapshot::METADATA_FILE,
                                super::snapshot::MAX_METADATA_BYTES + 28
                            ],
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )
                        .map_err(db_error)?;
                    let bytes = bytes.filter(|_| parts == 1).ok_or_else(|| {
                        Error::Snapshot("checkpoint counter metadata missing or oversized".into())
                    })?;
                    let bytes = self.decrypt(
                        &aad(
                            &captured,
                            checkpoint,
                            &binding_json,
                            super::snapshot::METADATA_FILE,
                            0,
                        )?,
                        bytes,
                    )?;
                    super::snapshot::instance_requests_served(&bytes, binding, &captured)?
                }
                (None, None) if state != super::InstanceState::Parked => 0,
                _ => {
                    return Err(Error::Snapshot(
                        "checkpoint counter metadata identity missing".into(),
                    ));
                }
            };
            records.push(CheckpointRecord {
                identity,
                state,
                checkpoint_id,
                requests_served,
            });
            if records.len() > 4096 {
                return Err(Error::Snapshot(
                    "checkpoint instance record retention limit exceeded".into(),
                ));
            }
        }
        Ok(records)
    }

    /// The key must come from an external secret store and remain available
    /// after home replacement; never put it in the checkpoint volume/package.
    pub fn open(path: impl AsRef<Path>, key: &[u8; 32], max_bytes: u64) -> Result<Self> {
        Self::open_with_scratch(path, key, max_bytes, None)
    }

    pub fn open_with_scratch(
        path: impl AsRef<Path>,
        key: &[u8; 32],
        max_bytes: u64,
        scratch: Option<&Path>,
    ) -> Result<Self> {
        if max_bytes == 0 || max_bytes > 8 * 1024 * 1024 * 1024 {
            return Err(Error::Snapshot("invalid checkpoint byte budget".into()));
        }
        let path = path.as_ref();
        match fs::symlink_metadata(path) {
            Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
                return Err(Error::Snapshot(
                    "checkpoint database must be a regular file".into(),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut options = OpenOptions::new();
                options.create_new(true).write(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                options.open(path)?.sync_all()?;
            }
            Err(error) => return Err(error.into()),
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
                return Err(Error::Snapshot(
                    "checkpoint database permissions must exclude group/other access".into(),
                ));
            }
        }
        let db = Connection::open(path).map_err(db_error)?;
        db.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(db_error)?;
        db.execute_batch(
            "PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS instances (
                id TEXT PRIMARY KEY, generation INTEGER NOT NULL, state TEXT NOT NULL,
                binding TEXT NOT NULL, checkpoint_id TEXT, checkpoint_generation INTEGER, owner TEXT);
             CREATE TABLE IF NOT EXISTS checkpoint_chunks (
                checkpoint_id TEXT NOT NULL, instance_id TEXT NOT NULL, path TEXT NOT NULL, part INTEGER NOT NULL,
                ciphertext BLOB NOT NULL, PRIMARY KEY(checkpoint_id,path,part));"
        ).map_err(db_error)?;
        let key = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, key).map_err(|_| crypto_error())?);
        let scratch_directory = scratch.map(std::fs::canonicalize).transpose()?;
        if let Some(scratch) = &scratch_directory
            && !std::fs::metadata(scratch)?.is_dir()
        {
            return Err(Error::Snapshot(
                "checkpoint scratch path must be a directory".into(),
            ));
        }
        Ok(Self {
            db: Mutex::new(db),
            key,
            max_bytes,
            scratch_directory,
        })
    }

    fn database(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.db
            .lock()
            .map_err(|_| Error::Snapshot("checkpoint database lock poisoned".into()))
    }

    pub fn register(&self, identity: &InstanceIdentity, binding: &SnapshotBinding) -> Result<()> {
        validate_identity(identity)?;
        if identity.generation != 1 {
            return Err(Error::Snapshot(
                "new instance must start at generation one".into(),
            ));
        }
        self.database()?
            .execute(
                "INSERT INTO instances(id,generation,state,binding) VALUES(?1,1,'active',?2)",
                params![identity.instance_id, serde_json::to_string(binding)?],
            )
            .map_err(db_error)?;
        Ok(())
    }

    /// Commits encrypted changed state but retains live ownership. The caller
    /// must terminate the actual VM before publishing PARKED.
    pub fn commit(&self, identity: &InstanceIdentity, image: &VerifiedSnapshot) -> Result<String> {
        validate_identity(identity)?;
        if !matches!(&image.purpose,super::snapshot::SnapshotPurpose::Instance {identity:captured,..}if captured==identity)
        {
            return Err(Error::Snapshot(
                "checkpoint must capture the exact changed instance identity and generation".into(),
            ));
        }
        let scratch = CheckpointLayout::new_in(self.scratch_directory.as_deref())?;
        let layout = scratch.path().join("snapshot");
        image.save(&layout)?;
        let mut paths = Vec::new();
        collect_files(&layout, &layout, &mut paths)?;
        let checkpoint_id = random_id()?;
        let mut db = self.database()?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let binding = serde_json::to_string(image.binding())?;
        let changed = tx.execute(
            "UPDATE instances SET state='checkpointing',checkpoint_id=?1,checkpoint_generation=?2
             WHERE id=?3 AND generation=?2 AND state='active' AND binding=?4",
            params![checkpoint_id, identity.generation, identity.instance_id, binding],
        ).map_err(db_error)?;
        require_one(changed)?;
        let mut total = 0u64;
        for path in paths {
            let mut file = File::open(layout.join(&path))?;
            let mut part = 0u64;
            loop {
                let mut bytes = vec![0; CHUNK_BYTES];
                let count = file.read(&mut bytes)?;
                if count == 0 {
                    // An empty file still needs an authenticated representation.
                    if part != 0 {
                        break;
                    }
                    bytes.clear();
                } else {
                    bytes.truncate(count);
                    total = total
                        .checked_add(count as u64)
                        .ok_or_else(|| Error::Snapshot("checkpoint size overflow".into()))?;
                    if total > self.max_bytes {
                        return Err(Error::Snapshot(
                            "checkpoint exceeds configured byte budget".into(),
                        ));
                    }
                }
                let aad = aad(identity, &checkpoint_id, &binding, &path, part)?;
                let encrypted = self.encrypt(&aad, bytes)?;
                tx.execute(
                    "INSERT INTO checkpoint_chunks(checkpoint_id,instance_id,path,part,ciphertext) VALUES(?1,?2,?3,?4,?5)",
                    params![checkpoint_id, identity.instance_id, path, part, encrypted],
                ).map_err(db_error)?;
                part += 1;
                if count == 0 {
                    break;
                }
            }
        }
        tx.commit().map_err(db_error)?;
        Ok(checkpoint_id)
    }

    pub fn publish_parked(&self, identity: &InstanceIdentity, checkpoint_id: &str) -> Result<()> {
        validate_identity(identity)?;
        let mut db = self.database()?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let changed = tx
            .execute(
                "UPDATE instances SET state='parked',owner=NULL
             WHERE id=?1 AND generation=?2 AND state='checkpointing' AND checkpoint_id=?3",
                params![identity.instance_id, identity.generation, checkpoint_id],
            )
            .map_err(db_error)?;
        require_one(changed)?;
        tx.execute(
            "DELETE FROM checkpoint_chunks WHERE instance_id=?1 AND checkpoint_id!=?2",
            params![identity.instance_id, checkpoint_id],
        )
        .map_err(db_error)?;
        tx.commit().map_err(db_error)?;
        Ok(())
    }

    pub fn claim(
        &self,
        identity: &InstanceIdentity,
        expected: &SnapshotBinding,
    ) -> Result<CheckpointClaim> {
        validate_identity(identity)?;
        let next_generation = identity
            .generation
            .checked_add(1)
            .filter(|generation| *generation <= i64::MAX as u64)
            .ok_or_else(|| Error::Snapshot("instance generation exhausted".into()))?;
        let binding = serde_json::to_string(expected)?;
        let owner = random_id()?;
        let (checkpoint_id, checkpoint_generation) = {
            let mut db = self.database()?;
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(db_error)?;
            let record: Option<(String, u64)> = tx
                .query_row(
                    "SELECT checkpoint_id,checkpoint_generation FROM instances
                 WHERE id=?1 AND generation=?2 AND state='parked' AND binding=?3",
                    params![identity.instance_id, identity.generation, binding],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(db_error)?;
            let record = record.ok_or_else(|| {
                Error::Snapshot("stale fence, busy owner or incompatible checkpoint".into())
            })?;
            require_one(
                tx.execute(
                    "UPDATE instances SET state='resuming',generation=?1,owner=?2
                 WHERE id=?3 AND generation=?4 AND state='parked'",
                    params![
                        next_generation,
                        owner,
                        identity.instance_id,
                        identity.generation
                    ],
                )
                .map_err(db_error)?,
            )?;
            tx.commit().map_err(db_error)?;
            record
        };
        let next = InstanceIdentity {
            instance_id: identity.instance_id.clone(),
            generation: next_generation,
        };
        let result = self.decrypt_layout(
            &InstanceIdentity {
                instance_id: identity.instance_id.clone(),
                generation: checkpoint_generation,
            },
            &checkpoint_id,
            &binding,
            expected,
        );
        match result {
            Ok((layout, image)) => Ok(CheckpointClaim {
                identity: next,
                checkpoint_id,
                owner,
                image,
                layout: Arc::new(layout),
            }),
            Err(error) => {
                self.finish_claim(&next, &owner, "failed")?;
                Err(error)
            }
        }
    }

    /// Publish ACTIVE only after the compatible guest and host adapters have
    /// actually been restored; a failed restore must call fail instead.
    pub fn activate(&self, claim: &CheckpointClaim) -> Result<()> {
        self.finish_claim(&claim.identity, &claim.owner, "active")
    }

    pub fn fail(&self, claim: &CheckpointClaim) -> Result<()> {
        self.finish_claim(&claim.identity, &claim.owner, "failed")
    }

    pub fn release(&self, identity: &InstanceIdentity) -> Result<()> {
        validate_identity(identity)?;
        let mut db = self.database()?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        require_one(
            tx.execute(
                "UPDATE instances SET state='released',owner=NULL
             WHERE id=?1 AND generation=?2 AND state IN ('active','parked','failed')",
                params![identity.instance_id, identity.generation],
            )
            .map_err(db_error)?,
        )?;
        tx.execute(
            "DELETE FROM checkpoint_chunks WHERE instance_id=?1",
            [&identity.instance_id],
        )
        .map_err(db_error)?;
        tx.commit().map_err(db_error)?;
        Ok(())
    }

    fn finish_claim(&self, identity: &InstanceIdentity, owner: &str, state: &str) -> Result<()> {
        require_one(self.database()?.execute(
            "UPDATE instances SET state=?1 WHERE id=?2 AND generation=?3 AND state='resuming' AND owner=?4",
            params![state, identity.instance_id, identity.generation, owner],
        ).map_err(db_error)?)
    }

    /// Explicit fenced deletion is allowed only when no running/resuming VM
    /// owns this instance. It also removes all superseded checkpoints.
    pub fn delete(&self, identity: &InstanceIdentity) -> Result<()> {
        validate_identity(identity)?;
        let mut db = self.database()?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let checkpoint: Option<String> = tx.query_row(
            "SELECT checkpoint_id FROM instances WHERE id=?1 AND generation=?2 AND state IN ('parked','failed')",
            params![identity.instance_id, identity.generation], |row| row.get(0),
        ).optional().map_err(db_error)?;
        checkpoint.ok_or_else(|| {
            Error::Snapshot("cannot delete a live instance or stale fence".into())
        })?;
        tx.execute(
            "DELETE FROM checkpoint_chunks WHERE instance_id=?1",
            [&identity.instance_id],
        )
        .map_err(db_error)?;
        require_one(
            tx.execute(
                "DELETE FROM instances WHERE id=?1 AND generation=?2",
                params![identity.instance_id, identity.generation],
            )
            .map_err(db_error)?,
        )?;
        tx.commit().map_err(db_error)?;
        Ok(())
    }

    fn encrypt(&self, aad: &[u8], mut bytes: Vec<u8>) -> Result<Vec<u8>> {
        let mut nonce = [0; 12];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| crypto_error())?;
        self.key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(aad),
                &mut bytes,
            )
            .map_err(|_| crypto_error())?;
        let mut result = nonce.to_vec();
        result.extend_from_slice(&bytes);
        Ok(result)
    }

    fn decrypt(&self, aad: &[u8], mut bytes: Vec<u8>) -> Result<Vec<u8>> {
        if bytes.len() < 28 || bytes.len() > CHUNK_BYTES + 28 {
            return Err(crypto_error());
        }
        let nonce: [u8; 12] = bytes[..12].try_into().map_err(|_| crypto_error())?;
        let plaintext = self
            .key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(aad),
                &mut bytes[12..],
            )
            .map_err(|_| crypto_error())?;
        Ok(plaintext.to_vec())
    }

    fn decrypt_layout(
        &self,
        identity: &InstanceIdentity,
        checkpoint_id: &str,
        binding: &str,
        expected: &SnapshotBinding,
    ) -> Result<(CheckpointLayout, VerifiedSnapshot)> {
        let layout = CheckpointLayout::new_in(self.scratch_directory.as_deref())?;
        let snapshot_path = layout.path().join("snapshot");
        fs::create_dir(&snapshot_path)?;
        let db = self.database()?;
        let (count, max_length): (u64, u64) = db.query_row(
            "SELECT count(*),coalesce(max(length(ciphertext)),0) FROM checkpoint_chunks WHERE checkpoint_id=?1",
            [checkpoint_id], |row| Ok((row.get(0)?, row.get(1)?)),
        ).map_err(db_error)?;
        if count == 0
            || count > self.max_bytes.div_ceil(CHUNK_BYTES as u64) + MAX_FILES as u64
            || max_length > (CHUNK_BYTES + 28) as u64
        {
            return Err(Error::Snapshot("invalid checkpoint chunk layout".into()));
        }
        let mut statement = db.prepare(
            "SELECT path,part,ciphertext FROM checkpoint_chunks WHERE checkpoint_id=?1 ORDER BY path,part"
        ).map_err(db_error)?;
        let mut rows = statement.query([checkpoint_id]).map_err(db_error)?;
        let mut current = None;
        let mut file: Option<File> = None;
        let mut next_part = 0u64;
        let mut total = 0u64;
        let mut files = 0usize;
        while let Some(row) = rows.next().map_err(db_error)? {
            let path: String = row.get(0).map_err(db_error)?;
            let part: u64 = row.get(1).map_err(db_error)?;
            validate_path(&path)?;
            if current.as_deref() != Some(path.as_str()) {
                if let Some(file) = file.take() {
                    file.sync_all()?;
                }
                files += 1;
                if files > MAX_FILES {
                    return Err(Error::Snapshot("checkpoint has too many files".into()));
                }
                let destination = snapshot_path.join(&path);
                if let Some(parent) = destination.parent() {
                    fs::create_dir_all(parent)?;
                }
                file = Some(
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(destination)?,
                );
                current = Some(path.clone());
                next_part = 0;
            }
            if part != next_part {
                return Err(Error::Snapshot(
                    "checkpoint chunks missing or reordered".into(),
                ));
            }
            let encrypted: Vec<u8> = row.get(2).map_err(db_error)?;
            let bytes = self.decrypt(
                &aad(identity, checkpoint_id, binding, &path, part)?,
                encrypted,
            )?;
            total = total
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| Error::Snapshot("checkpoint byte overflow".into()))?;
            if total > self.max_bytes {
                return Err(Error::Snapshot(
                    "checkpoint exceeds configured byte budget".into(),
                ));
            }
            file.as_mut()
                .ok_or_else(|| Error::Snapshot("checkpoint file missing".into()))?
                .write_all(&bytes)?;
            next_part += 1;
        }
        if let Some(file) = file.take() {
            file.sync_all()?;
        }
        super::snapshot::seal_or_check(&snapshot_path, true)?;
        let image = VerifiedSnapshot::open(&snapshot_path, expected)?;
        Ok((layout, image))
    }
}

fn validate_identity(identity: &InstanceIdentity) -> Result<()> {
    if identity.instance_id.is_empty()
        || identity.instance_id.len() > 256
        || !identity
            .instance_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
        || identity.generation == 0
        || identity.generation > i64::MAX as u64
    {
        return Err(Error::Snapshot(
            "invalid instance identity or generation".into(),
        ));
    }
    Ok(())
}

fn validate_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.len() > 512
        || path.contains('\\')
        || Path::new(path)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(Error::Snapshot("invalid checkpoint artifact path".into()));
    }
    Ok(())
}

fn collect_files(root: &Path, directory: &Path, paths: &mut Vec<String>) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let metadata = entry.file_type()?;
        if metadata.is_dir() {
            collect_files(root, &entry.path(), paths)?;
        } else if metadata.is_file() {
            if paths.len() >= MAX_FILES {
                return Err(Error::Snapshot("checkpoint has too many files".into()));
            }
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .map_err(|error| Error::Snapshot(error.to_string()))?
                .to_str()
                .ok_or_else(|| Error::Snapshot("non-UTF8 checkpoint artifact path".into()))?
                .replace('\\', "/");
            validate_path(&relative)?;
            paths.push(relative);
        } else {
            return Err(Error::Snapshot(
                "checkpoint layout contains a link or special file".into(),
            ));
        }
    }

    paths.sort();
    Ok(())
}

fn unseal_private(path: &Path) -> std::io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(std::io::Error::other("private layout contains a symlink"));
    }
    let mut permissions = metadata.permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(if metadata.is_dir() { 0o700 } else { 0o600 });
    }
    #[cfg(not(unix))]
    #[expect(
        clippy::permissions_set_readonly_false,
        reason = "Clear non-Unix readonly attributes for cleanup; Unix uses private mode bits above"
    )]
    permissions.set_readonly(false);
    fs::set_permissions(path, permissions)?;
    if metadata.is_dir() {
        for entry in fs::read_dir(path)? {
            unseal_private(&entry?.path())?;
        }
    }
    Ok(())
}

fn aad(
    identity: &InstanceIdentity,
    checkpoint: &str,
    binding: &str,
    path: &str,
    part: u64,
) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&(
        "hyperloom-instance-checkpoint-v1",
        identity,
        checkpoint,
        binding,
        path,
        part,
    ))?)
}

fn require_one(changed: usize) -> Result<()> {
    if changed == 1 {
        Ok(())
    } else {
        Err(Error::Snapshot(
            "stale instance fence or invalid lifecycle state".into(),
        ))
    }
}

fn db_error(error: rusqlite::Error) -> Error {
    Error::Snapshot(format!("checkpoint database: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authenticated_encryption_rejects_corruption_wrong_key_and_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CheckpointStore::open(tmp.path().join("one.db"), &[1; 32], 4096).unwrap();
        let wrong = CheckpointStore::open(tmp.path().join("two.db"), &[2; 32], 4096).unwrap();
        let encrypted = store
            .encrypt(
                b"instance-generation-target-policy",
                b"changed heap secret".to_vec(),
            )
            .unwrap();
        assert!(
            !encrypted
                .windows(19)
                .any(|window| window == b"changed heap secret")
        );
        assert_eq!(
            store
                .decrypt(b"instance-generation-target-policy", encrypted.clone())
                .unwrap(),
            b"changed heap secret"
        );
        wrong
            .decrypt(b"instance-generation-target-policy", encrypted.clone())
            .unwrap_err();
        store
            .decrypt(b"wrong-generation", encrypted.clone())
            .unwrap_err();
        let mut corrupt = encrypted;
        corrupt[12] ^= 1;
        store
            .decrypt(b"instance-generation-target-policy", corrupt)
            .unwrap_err();
    }

    #[test]
    fn sealed_plaintext_layout_is_removed_on_drop() {
        let layout = CheckpointLayout::new().unwrap();
        let path = layout.path().to_path_buf();
        fs::create_dir(path.join("sealed")).unwrap();
        fs::write(path.join("sealed").join("secret"), b"guest heap secret").unwrap();
        super::super::snapshot::seal_or_check(&path, true).unwrap();
        drop(layout);
        assert!(!path.exists());
    }
}

//! Bounded host-owned data capabilities for Workerd request VMs.
//!
//! Each capability is an independent logical broker service. Guest payloads
//! select only an explicitly registered logical binding; host paths, database
//! handles, workload identity, and quota configuration never cross the wire.

mod cache;
mod d1;
mod durable;
mod kv;

pub use cache::{
    CacheBinding, CacheClock, CacheLimits, CacheOperation, CacheRequest, CacheResponse,
    CacheService, CacheStatus, SystemCacheClock,
};
pub use d1::{
    D1Binding, D1Limits, D1Operation, D1Parameter, D1Request, D1Response, D1ResultMetadata,
    D1ResultSet, D1Service, D1Statement, D1Status, D1Value,
};
pub use d1::{
    D1Binding as SqlBinding, D1Limits as SqlLimits, D1Operation as SqlOperation,
    D1Parameter as SqlParameter, D1ResultMetadata as SqlResultMetadata,
    D1ResultSet as SqlResultSet, D1Service as SqlService, D1Statement as SqlStatement,
    D1Value as SqlValue, sql_api_response_bound,
};
pub use durable::{
    DurableDeliveryContext, DurableObjectBinding, DurableObjectError, DurableObjectLimits,
    DurableObjectOperation, DurableObjectRequest, DurableObjectResponse, DurableObjectService,
    DurableObjectStatus,
};
pub use kv::{
    KvAuditEvent, KvBinding, KvDecision, KvLimits, KvOperation, KvRequest, KvResponse, KvService,
    KvStatus,
};

use std::path::{Path, PathBuf};

const MAX_BINDING_NAME_BYTES: usize = 64;
const MAX_REQUEST_ID_BYTES: usize = 64;
const MAX_KEY_BYTES: usize = 1024;
const MAX_CACHE_KEY_BYTES: usize = 4096;

#[derive(Debug, thiserror::Error)]
pub enum DataError {
    #[error("invalid data binding name")]
    InvalidBinding,
    #[error("SQL API number must be finite and integral values must be JavaScript-safe")]
    InvalidApiNumber,
    #[error("SQL API result requires finite measured duration and JavaScript-safe metadata")]
    InvalidApiMetadata,
    #[error("SQL API result blob must use standard base64")]
    InvalidApiBlob,
    #[error("invalid data request ID")]
    InvalidRequestId,
    #[error("invalid data key")]
    InvalidKey,
    #[error("data limit must be nonzero")]
    InvalidLimit,
    #[error("failed to resolve data backing parent {path}")]
    ResolveBackingParent {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("data backing path has no file name")]
    InvalidBackingPath,
    #[error("failed to open SQLite data backing {path}")]
    OpenBacking {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },
    #[error("failed to initialize SQLite data backing")]
    InitializeBacking(#[source] rusqlite::Error),
}

pub type Result<T> = std::result::Result<T, DataError>;

fn validate_binding(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_BINDING_NAME_BYTES
        || value == "__proto__"
        || !value.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_alphabetic()
                || byte == b'_'
                || (index > 0 && (byte.is_ascii_digit() || byte == b'-'))
        })
    {
        return Err(DataError::InvalidBinding);
    }
    Ok(())
}

fn validate_request_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_REQUEST_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        return Err(DataError::InvalidRequestId);
    }
    Ok(())
}

fn validate_key(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_KEY_BYTES
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == 0)
    {
        return Err(DataError::InvalidKey);
    }
    Ok(())
}

fn validate_cache_key(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_CACHE_KEY_BYTES
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(DataError::InvalidKey);
    }
    Ok(())
}

fn canonical_backing_path(path: &Path) -> Result<PathBuf> {
    let Some(file_name) = path.file_name() else {
        return Err(DataError::InvalidBackingPath);
    };
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let parent =
        std::fs::canonicalize(parent).map_err(|source| DataError::ResolveBackingParent {
            path: parent.to_path_buf(),
            source,
        })?;
    Ok(parent.join(file_name))
}

fn safe_code(value: &str) -> &str {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        "host_error"
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_names_preserve_ascii_case_and_legacy_spelling() {
        for name in [
            "DB",
            "Store",
            "_DB",
            "_",
            "store",
            "legacy-store",
            "store_1",
            "constructor",
            "prototype",
        ] {
            assert!(validate_binding(name).is_ok(), "{name}");
            assert_eq!(
                D1Binding::in_memory(name, false, D1Limits::default())
                    .unwrap()
                    .name(),
                name
            );
        }
        assert!(validate_binding(&"A".repeat(MAX_BINDING_NAME_BYTES)).is_ok());
    }

    #[test]
    fn binding_names_reject_unsafe_syntax_and_keep_the_existing_byte_bound() {
        for name in [
            "",
            "1DB",
            "-DB",
            "__proto__",
            "DB.name",
            "DB/name",
            "DB\\name",
            "DB:name",
            "DB name",
            "DB\n",
            "DB\r",
            "DB\0",
            "\u{00c9}DB",
        ] {
            assert!(validate_binding(name).is_err(), "{name:?}");
        }
        assert!(validate_binding(&"A".repeat(MAX_BINDING_NAME_BYTES + 1)).is_err());
    }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{Error, PACKAGE_PROTOCOL_VERSION, Result, WorkerBundle, WorkerVersionId};
use hyperlight_host::func::Registerable;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub const BUNDLE_READ_BYTES: usize = 16 * 1024;

#[derive(Clone, Default)]
pub(super) struct BundleReader {
    inner: Option<Arc<BundleReaderInner>>,
}

struct BundleReaderInner {
    version: WorkerVersionId,
    digest: String,
    sources: HashMap<String, Vec<u8>>,
    active: AtomicBool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadQuery {
    protocol_version: u16,
    worker_version: WorkerVersionId,
    bundle_sha256: String,
    module_name: String,
    offset: u32,
    max_bytes: u32,
}

impl BundleReader {
    pub(super) fn new(bundle: &WorkerBundle) -> Result<Self> {
        if bundle.protocol_version != PACKAGE_PROTOCOL_VERSION {
            return Ok(Self::default());
        }
        bundle.validate()?;
        let sources = bundle
            .modules
            .iter()
            .map(|module| Ok((module.name.clone(), module.source_bytes()?)))
            .collect::<Result<HashMap<_, _>>>()?;
        Ok(Self {
            inner: Some(Arc::new(BundleReaderInner {
                version: bundle.worker_version.clone(),
                digest: bundle.sha256()?,
                sources,
                active: AtomicBool::new(true),
            })),
        })
    }

    pub(super) fn close(&self) {
        if let Some(inner) = &self.inner {
            inner.active.store(false, Ordering::Release);
        }
    }

    fn read(&self, query: &str) -> Result<Vec<u8>> {
        if query.len() > 4096 {
            return Err(Error::Protocol("bundle read query exceeds limit".into()));
        }
        let query: ReadQuery = serde_json::from_str(query)?;
        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| Error::State("chunked bundle capability denied".into()))?;
        if !inner.active.load(Ordering::Acquire)
            || query.protocol_version != 1
            || query.worker_version != inner.version
            || query.bundle_sha256 != inner.digest
            || query.max_bytes == 0
            || query.max_bytes as usize > BUNDLE_READ_BYTES
        {
            return Err(Error::Protocol(
                "bundle read authority or bounds mismatch".into(),
            ));
        }
        let source = inner
            .sources
            .get(&query.module_name)
            .ok_or_else(|| Error::Protocol("unknown immutable package module".into()))?;
        let offset = query.offset as usize;
        let end = offset
            .checked_add(query.max_bytes as usize)
            .ok_or_else(|| Error::Protocol("bundle read offset overflow".into()))?;
        let bytes = source.get(offset..end).ok_or_else(|| {
            Error::Protocol("bundle read beyond declared source or short read".into())
        })?;
        Ok(bytes.to_vec())
    }

    pub(super) fn register(&self, target: &mut impl Registerable) -> Result<()> {
        let reader = self.clone();
        target.register_host_function(
            "WorkerdBundleV1Read",
            move |query: String| -> hyperlight_host::Result<Vec<u8>> {
                reader
                    .read(&query)
                    .map_err(|error| hyperlight_host::new_error!("{error}"))
            },
        )?;
        Ok(())
    }
}

#[derive(Serialize)]
pub(super) struct ModuleDescriptor<'a> {
    name: &'a str,
    #[serde(rename = "type")]
    module_type: super::ModuleType,
    source_bytes: u32,
    source_sha256: String,
}

pub(super) fn descriptor(module: &super::WorkerModule) -> Result<ModuleDescriptor<'_>> {
    let source = module.source_bytes()?;
    let digest: String = Sha256::digest(&source)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(ModuleDescriptor {
        name: &module.name,
        module_type: module.module_type,
        source_bytes: u32::try_from(source.len())
            .map_err(|_| Error::Protocol("module length exceeds u32".into()))?,
        source_sha256: digest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_explicit_package_uses_exact_bounded_reads_and_denies_after_init() {
        let mut bundle = WorkerBundle::single_script(
            WorkerVersionId::new("large-v4").unwrap(),
            "2025-01-01",
            "worker.js",
            "export default {}",
        )
        .unwrap();
        bundle.protocol_version = PACKAGE_PROTOCOL_VERSION;
        bundle.modules[0].source = "x".repeat(757597);
        bundle.validate().unwrap();
        let reader = BundleReader::new(&bundle).unwrap();
        let mut query = serde_json::json!({
            "protocol_version":1,"worker_version":"large-v4","bundle_sha256":bundle.sha256().unwrap(),
            "module_name":"worker.js","offset":0,"max_bytes":BUNDLE_READ_BYTES,
        });
        assert_eq!(
            reader.read(&query.to_string()).unwrap().len(),
            BUNDLE_READ_BYTES
        );
        query["offset"] = serde_json::json!(757596);
        query["max_bytes"] = serde_json::json!(1);
        assert_eq!(reader.read(&query.to_string()).unwrap(), b"x");
        query["max_bytes"] = serde_json::json!(2);
        reader.read(&query.to_string()).unwrap_err();
        query["max_bytes"] = serde_json::json!(1);
        query["bundle_sha256"] = serde_json::json!("0".repeat(64));
        reader.read(&query.to_string()).unwrap_err();
        query["bundle_sha256"] = serde_json::json!(bundle.sha256().unwrap());
        reader.close();
        reader.read(&query.to_string()).unwrap_err();
        bundle.protocol_version = 1;
        bundle.validate().unwrap_err();
    }
}

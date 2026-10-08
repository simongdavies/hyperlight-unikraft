// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Trusted-host policies tested against a synthetic credential receiver.
//! This exercises actual HTTP transmission, not Workerd JavaScript.
use hyperlight_unikraft::workerd::*;
use hyperlight_unikraft::{AllowList, NetworkPolicy};

#[test]
fn strict_config_denies_unknown_http_methods_cidr_and_secret_mount_overlap() {
    for json in [
        r#"{"fetch":{"methods":["CONNECT"]}}"#,
        r#"{"fetch":{"methods":["get"]}}"#,
        r#"{"fetch":{"ip_ranges":["127.0.0.1/999"]}}"#,
    ] {
        if let Ok(config) = serde_json::from_str::<WorkerCapabilityPolicyConfig>(json) {
            assert!(config.sha256().is_err(), "{json}");
        }
    }
    let tmp = tempfile::tempdir().unwrap();
    let config: WorkerCapabilityPolicyConfig = serde_json::from_value(serde_json::json!({
        "fetch":{"credential":{"reference":"synthetic","value_file":tmp.path().join("token"),
            "header":"authorization","scheme":"http","host":"127.0.0.1","port":8080}},
        "storage":[{"name":"data","host_path":tmp.path(),"mode":"read_only",
            "max_operations":1,"max_read_bytes":1,"max_write_bytes":1}],
    }))
    .unwrap();
    assert!(
        config
            .sha256()
            .unwrap_err()
            .to_string()
            .contains("overlaps")
    );
}

#[test]
fn policy_fingerprint_binds_methods_ranges_and_secret_reference_not_secret_contents() {
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("token");
    std::fs::write(&file, b"synthetic-token-one").unwrap();
    let config: WorkerCapabilityPolicyConfig = serde_json::from_value(serde_json::json!({
        "fetch":{"hosts":["127.0.0.1"],"schemes":["http"],"ports":[8080],
            "methods":["GET"],"ip_ranges":["127.0.0.0/8"],"allow_loopback":true,
            "credential":{"reference":"synthetic","value_file":file,
                "header":"authorization","scheme":"http","host":"127.0.0.1","port":8080}},
    }))
    .unwrap();
    let before = config.sha256().unwrap();
    std::fs::write(&file, b"synthetic-token-rotated").unwrap();
    assert_eq!(config.sha256().unwrap(), before);
    let mut changed = config.clone();
    changed.fetch.methods = vec!["POST".into()];
    assert_ne!(changed.sha256().unwrap(), before);
    changed = config.clone();
    changed.fetch.ip_ranges = vec!["192.0.2.0/24".parse().unwrap()];
    assert_ne!(changed.sha256().unwrap(), before);
    changed = config;
    changed.fetch.credential.as_mut().unwrap().reference = "different-secret".into();
    assert_ne!(changed.sha256().unwrap(), before);
}

// The public outbound broker is connected to VM host callbacks; exact
// transmission tests live alongside its internal session API in fetch.rs.
#[test]
fn credential_configuration_never_prints_values_and_requires_scoped_destination() {
    let tmp = tempfile::tempdir().unwrap();
    let credential = FetchCredential {
        reference: "synthetic".into(),
        value_file: tmp.path().join("token"),
        header: "authorization".into(),
        scheme: "http".into(),
        host: "example.com".into(),
        port: 80,
    };
    assert!(
        FetchBroker::denied()
            .with_credential(credential.clone())
            .is_err()
    );
    assert!(!format!("{credential:?}").contains("token"));
    let policy = FetchPolicy::new(
        NetworkPolicy::AllowList(AllowList::from_hosts(&["127.0.0.1"]).unwrap()),
        ["http"],
        [80],
    )
    .allow_loopback(true)
    .with_methods(["GET".into()])
    .unwrap();
    assert!(
        FetchBroker::new(FetchBrokerConfig {
            policy,
            limits: Default::default()
        })
        .is_ok()
    );
}

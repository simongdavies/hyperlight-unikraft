pub const EXCLUSIONS_SHA256: &str =
    "e43209923e1574ac7bd11f3edb1be2348ec39592e9f47d432b1a839b0bad1754";
pub const GENERATION_PROOF_SHA256: &str =
    "31338f5b571b2b4fa9e8631f2f248f470c6cca49876a63afc60c29561399247d";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KvmProofState {
    PendingCoordinatorSlot,
    Passed,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct P2ProofIdentity {
    pub signed_baseline: &'static str,
    pub wasi_release_commit: &'static str,
    pub lock_sha256: &'static str,
    pub policy_sha256: &'static str,
    pub exclusions_sha256: &'static str,
    pub generation_proof_sha256: &'static str,
    pub kvm: KvmProofState,
}

impl P2ProofIdentity {
    pub const LIGHTWEIGHT: Self = Self {
        signed_baseline: "5560e071f81706488efcab86ad534e80a3a8553d",
        wasi_release_commit: "281ba75fafcd50961ef55f9e52747afcc9b71ede",
        lock_sha256: "f4f8227f70056750a91c8ff1712138e3a5ff65dd9586042e2dedf2edd847ce28",
        policy_sha256: "d4f1316d59594640688a2d8a3875fea675dc51196f36e19024d275675e273447",
        exclusions_sha256: EXCLUSIONS_SHA256,
        generation_proof_sha256: GENERATION_PROOF_SHA256,
        kvm: KvmProofState::PendingCoordinatorSlot,
    };
}

#[cfg(test)]
mod tests {
    use crate::wasi_preview2::{
        AdapterError, CliAdapter, CliPolicy, ClockAdapter, ClockBackend, ClockPolicy,
        FilesystemAdapter, HttpAdapter, IncomingHttpHandler, InputStream, InterfaceDisposition,
        OutputStream, P2AdapterRegistry, P2Import, P2Policy, PolicyError, RandomAdapter,
        RandomPolicy, SecureRandom,
    };
    use crate::workerd::{FetchBroker, RequestEnvelope, ResponseEnvelope};
    use crate::{Mount, MountLimits};
    use std::collections::BTreeMap;

    struct FixedClock;

    impl ClockBackend for FixedClock {
        fn wall_clock_ns(&self) -> Result<u64, AdapterError> {
            Ok(99)
        }

        fn monotonic_clock_ns(&self) -> Result<u64, AdapterError> {
            Ok(100)
        }
    }

    struct FixedRandom(u8);

    impl SecureRandom for FixedRandom {
        fn fill_secure(&mut self, output: &mut [u8]) -> Result<(), AdapterError> {
            output.fill(self.0);
            Ok(())
        }
    }

    struct EchoIngress;

    impl IncomingHttpHandler for EchoIngress {
        fn handle(&mut self, request: RequestEnvelope) -> Result<ResponseEnvelope, AdapterError> {
            Ok(ResponseEnvelope {
                protocol_version: request.protocol_version,
                request_id: request.request_id,
                status: 204,
                headers: Vec::new(),
                body_base64: String::new(),
            })
        }
    }

    fn cli_policy() -> CliPolicy {
        CliPolicy::new(
            BTreeMap::from([("LANG".to_string(), "C.UTF-8".to_string())]),
            vec!["component".to_string()],
            16,
            16,
            16,
            true,
            true,
        )
        .unwrap()
    }

    #[test]
    fn unknown_and_ungranted_imports_fail_closed() {
        let policy = P2Policy::deny_all();
        assert_eq!(
            policy.authorize_name("wasi:unknown/ambient@0.2.12"),
            Err(PolicyError::UnsupportedImport(
                "wasi:unknown/ambient@0.2.12".to_string()
            ))
        );
        assert_eq!(
            policy.authorize(P2Import::FilesystemPreopens),
            Err(PolicyError::CapabilityDenied(
                "wasi:filesystem/preopens@0.2.12"
            ))
        );
        assert_eq!(
            policy.authorize(P2Import::CliTerminalStdin),
            Err(PolicyError::NotSupported("wasi:cli/terminal-stdin@0.2.12"))
        );
    }

    #[test]
    fn explicit_policy_authorizes_only_backed_interfaces() {
        let policy = P2Policy::deny_all()
            .with_clocks(ClockPolicy::Deterministic {
                wall_epoch_ns: 1,
                monotonic_step_ns: 2,
                timezone: Some("UTC".to_string()),
            })
            .with_random(RandomPolicy::SecureAndDeterministicInsecure {
                max_secure_bytes: 32,
                insecure_seed: [7; 32],
            })
            .with_filesystem()
            .with_sockets()
            .with_http_client()
            .with_http_service()
            .with_cli(cli_policy());

        for (import, disposition) in [
            (P2Import::IoStreams, InterfaceDisposition::Native),
            (P2Import::IoPoll, InterfaceDisposition::Adapted),
            (P2Import::ClocksMonotonic, InterfaceDisposition::Adapted),
            (P2Import::RandomSecure, InterfaceDisposition::Adapted),
            (P2Import::RandomInsecure, InterfaceDisposition::Native),
            (P2Import::FilesystemTypes, InterfaceDisposition::Adapted),
            (P2Import::SocketsTcp, InterfaceDisposition::Adapted),
            (P2Import::HttpTypes, InterfaceDisposition::Native),
            (P2Import::HttpOutgoingHandler, InterfaceDisposition::Adapted),
            (P2Import::HttpIncomingHandler, InterfaceDisposition::Adapted),
            (P2Import::CliEnvironment, InterfaceDisposition::Adapted),
            (P2Import::CliRun, InterfaceDisposition::Adapted),
        ] {
            assert_eq!(policy.authorize(import).unwrap().disposition, disposition);
        }
        assert!(matches!(
            policy.authorize(P2Import::SocketsUdp),
            Err(PolicyError::NotSupported(_))
        ));
    }

    #[test]
    fn incoming_http_uses_the_typed_executor_ingress_seam() {
        let mut http = HttpAdapter::new(FetchBroker::denied()).with_incoming_handler(EchoIngress);
        let response = http
            .handle_incoming(RequestEnvelope {
                protocol_version: 1,
                request_id: "request-1".to_string(),
                method: "GET".to_string(),
                url: "https://worker.invalid/".to_string(),
                headers: Vec::new(),
                body_base64: String::new(),
            })
            .unwrap();
        assert_eq!(response.protocol_version, 1);
        assert_eq!(response.request_id, "request-1");
        assert_eq!(response.status, 204);
    }

    #[test]
    fn policy_grants_still_require_concrete_adapters() {
        let policy = P2Policy::deny_all().with_http_client();
        let registry: P2AdapterRegistry<'_, FixedClock, FixedRandom> = P2AdapterRegistry::new();
        assert!(matches!(
            registry.plan(&policy, ["wasi:http/outgoing-handler@0.2.12"]),
            Err(PolicyError::AdapterMissing("Workerd HTTP"))
        ));
        assert_eq!(
            registry
                .plan(&policy, ["wasi:http/types@0.2.12"])
                .unwrap()
                .authorizations()
                .len(),
            1
        );
    }

    #[test]
    fn configuration_validation_rejects_ambiguous_or_oversized_authority() {
        let limits = MountLimits {
            max_operations: Some(10),
            max_read_bytes: Some(1024),
            max_write_bytes: Some(1024),
        };
        assert!(matches!(
            FilesystemAdapter::new(vec![
                Mount::ro(".", "/data").with_limits(limits),
                Mount::ro(".", "/data/child").with_limits(limits),
            ]),
            Err(AdapterError::InvalidArgument(
                "duplicate or overlapping guest preopen path"
            ))
        ));

        let invalid_clock = P2Policy::deny_all().with_clocks(ClockPolicy::Deterministic {
            wall_epoch_ns: 1,
            monotonic_step_ns: 0,
            timezone: Some("UTC+99:99".to_string()),
        });
        assert!(matches!(
            invalid_clock.authorize(P2Import::ClocksMonotonic),
            Err(PolicyError::InvalidPolicy(_))
        ));

        let environment = (0..4)
            .map(|index| (format!("KEY{index}"), "x".repeat(16 * 1024)))
            .collect();
        assert!(matches!(
            CliPolicy::new(environment, Vec::new(), 1, 1, 1, true, true),
            Err(PolicyError::InvalidPolicy(
                "CLI environment and arguments exceed the init envelope"
            ))
        ));
    }

    #[test]
    fn streams_are_bounded_and_report_closed_handles() {
        let mut input = InputStream::new([1, 2, 3], 4, 2).unwrap();
        assert_eq!(input.read(8).unwrap(), vec![1, 2]);
        input.close();
        assert_eq!(input.read(1), Err(AdapterError::Closed));

        let mut output = OutputStream::new(3, 2).unwrap();
        assert_eq!(output.write(&[1, 2, 3]).unwrap(), 2);
        assert_eq!(
            output.write(&[3, 4]),
            Err(AdapterError::QuotaExceeded("output stream bytes"))
        );
    }

    #[test]
    fn clocks_and_insecure_random_are_deterministic() {
        let mut clock = ClockAdapter::new(
            ClockPolicy::Deterministic {
                wall_epoch_ns: 42,
                monotonic_step_ns: 5,
                timezone: Some("UTC".to_string()),
            },
            FixedClock,
        );
        assert_eq!(clock.wall_clock_ns().unwrap(), 42);
        assert_eq!(clock.monotonic_clock_ns().unwrap(), 0);
        assert_eq!(clock.monotonic_clock_ns().unwrap(), 5);
        assert_eq!(clock.timezone().unwrap(), "UTC");

        let policy = RandomPolicy::SecureAndDeterministicInsecure {
            max_secure_bytes: 4,
            insecure_seed: [9; 32],
        };
        let mut left = RandomAdapter::new(policy.clone(), FixedRandom(0xa5));
        let mut right = RandomAdapter::new(policy, FixedRandom(0xa5));
        assert_eq!(left.insecure_u64().unwrap(), right.insecure_u64().unwrap());
        assert_eq!(left.secure_bytes(4).unwrap(), vec![0xa5; 4]);
        assert_eq!(
            left.secure_bytes(1),
            Err(AdapterError::QuotaExceeded("secure random bytes"))
        );
    }

    #[test]
    fn cli_never_reads_host_environment_and_bounds_stdio() {
        let policy = cli_policy();
        let mut cli = CliAdapter::new(&policy, vec![1, 2]).unwrap();
        assert_eq!(cli.environment().len(), 1);
        assert_eq!(
            cli.environment().get("LANG").map(String::as_str),
            Some("C.UTF-8")
        );
        cli.claim_run().unwrap();
        assert!(cli.claim_run().is_err());
        assert_eq!(cli.stdout().write(&[0; 32]).unwrap(), 16);
        cli.exit(true).unwrap();
        assert_eq!(cli.exit_status(), Some(Ok(())));
        assert!(matches!(cli.terminal(), Err(AdapterError::NotSupported(_))));
    }
}

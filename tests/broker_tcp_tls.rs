// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use hyperlight_unikraft::broker::{
    BrokerEndpoint, BrokerLimits, BrokerOperation, BrokerPolicy, BrokerProtocol, BrokerRequest,
    BrokerRequestId, DnsPolicy, EgressRule, EndpointHost, HostRule, PortRange, RequestIdentity,
    TlsProfile, TlsVersion,
};
use hyperlight_unikraft::broker_adapter::{BrokerAuditAction, BrokerAuditOutcome};
use hyperlight_unikraft::broker_network::{NetworkBroker, NetworkBrokerConfig};
use hyperlight_unikraft::broker_wire::{
    BrokerWireResult, BrokerWireStatus, decode_response, encode_request,
};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, TcpListener, UdpSocket};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn host_ip() -> Ipv4Addr {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect("8.8.8.8:80").unwrap();
    match socket.local_addr().unwrap().ip() {
        IpAddr::V4(ip) => ip,
        IpAddr::V6(ip) => panic!("expected IPv4 test host, got {ip}"),
    }
}

fn limits() -> BrokerLimits {
    BrokerLimits {
        max_connections: 2,
        max_sockets: 2,
        max_datagrams: 2,
        max_datagram_bytes: 1024,
        max_stream_bytes: 1024,
        max_messages: 2,
        max_message_bytes: 1024,
        max_bytes: 4096,
        max_elapsed: Duration::from_secs(10),
        max_operations: 16,
        max_operations_per_window: 16,
        rate_window: Duration::from_secs(1),
        max_concurrency: 1,
    }
}

fn identity() -> RequestIdentity {
    RequestIdentity::new("tcp-tls-fixture", "snapshot-a", 0).unwrap()
}

fn request(id: &str, operation: BrokerOperation) -> Vec<u8> {
    encode_request(&BrokerRequest::new(
        BrokerRequestId::new(id).unwrap(),
        operation,
    ))
    .unwrap()
}

fn dispatch(
    runtime: &hyperlight_unikraft::broker_runtime::BrokerRuntime,
    id: &str,
    operation: BrokerOperation,
) -> hyperlight_unikraft::broker_wire::BrokerWireResponse {
    decode_response(&runtime.dispatch_network(&request(id, operation))).unwrap()
}

fn policy(ip: Ipv4Addr, port: u16, protocols: Vec<BrokerProtocol>) -> BrokerPolicy {
    BrokerPolicy::new(
        vec![
            EgressRule::new(
                HostRule::Ip(IpAddr::V4(ip)),
                vec![PortRange::new(port, port).unwrap()],
                protocols,
            )
            .unwrap(),
        ],
        DnsPolicy::Deny,
    )
}

#[test]
fn tcp_stream_is_policy_bound_bounded_audited_and_reset() {
    let ip = host_ip();
    let listener = TcpListener::bind((ip, 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut bytes = [0; 4];
        stream.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"ping");
        stream.write_all(b"pong").unwrap();
    });
    let config = NetworkBrokerConfig {
        policy: policy(ip, port, vec![BrokerProtocol::Tcp]),
        limits: limits(),
        resolver: IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
        connect_timeout: Duration::from_secs(2),
        io_timeout: Duration::from_secs(2),
        tls_roots: Arc::new(rustls::RootCertStore::empty()),
    };
    let runtime = NetworkBroker::new(config)
        .unwrap()
        .runtime(identity())
        .unwrap();
    let endpoint = BrokerEndpoint::new(EndpointHost::Ip(IpAddr::V4(ip)), port).unwrap();
    let opened = dispatch(
        &runtime,
        "tcp-open",
        BrokerOperation::TcpConnect {
            endpoint: endpoint.clone(),
        },
    );
    let BrokerWireResult::Opened { handle_id } = opened.result() else {
        panic!("unexpected open result: {:?}", opened.result());
    };
    assert_eq!(opened.status(), BrokerWireStatus::Ok);
    assert_eq!(
        dispatch(
            &runtime,
            "tcp-send",
            BrokerOperation::StreamSend {
                stream_id: *handle_id,
                payload: b"ping".to_vec(),
            },
        )
        .result(),
        &BrokerWireResult::Transferred { bytes: 4 }
    );
    assert_eq!(
        dispatch(
            &runtime,
            "tcp-receive",
            BrokerOperation::StreamReceive {
                stream_id: *handle_id,
                max_bytes: 4,
            },
        )
        .result(),
        &BrokerWireResult::Received {
            payload: b"pong".to_vec(),
            binary: true,
        }
    );
    let audit = runtime.drain_network_audit().unwrap();
    assert_eq!(
        audit
            .iter()
            .map(|event| (event.action(), event.outcome()))
            .collect::<Vec<_>>(),
        [
            (BrokerAuditAction::TcpConnect, BrokerAuditOutcome::Ok),
            (BrokerAuditAction::StreamSend, BrokerAuditOutcome::Ok),
            (BrokerAuditAction::StreamReceive, BrokerAuditOutcome::Ok),
        ]
    );
    runtime.reset_for_fresh_vm().unwrap();
    let rejected = dispatch(
        &runtime,
        "tcp-after-reset",
        BrokerOperation::StreamSend {
            stream_id: *handle_id,
            payload: b"x".to_vec(),
        },
    );
    assert_eq!(
        (rejected.status(), rejected.code()),
        (
            BrokerWireStatus::InvalidRequest,
            Some("invalid_stream_handle")
        )
    );
    server.join().unwrap();
}

#[test]
fn tls_is_host_terminated_with_host_owned_roots() {
    let ip = host_ip();
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["fixture.test".to_string()]).unwrap();
    let certificate: CertificateDer<'static> = cert.der().clone();
    let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], private_key)
        .unwrap();
    let listener = TcpListener::bind((ip, 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let connection = rustls::ServerConnection::new(Arc::new(server_config)).unwrap();
        let mut stream = rustls::StreamOwned::new(connection, stream);
        let mut bytes = [0; 4];
        stream.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"ping");
        stream.write_all(b"pong").unwrap();
    });
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).unwrap();
    let config = NetworkBrokerConfig {
        policy: policy(ip, port, vec![BrokerProtocol::Tls]),
        limits: limits(),
        resolver: IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
        connect_timeout: Duration::from_secs(2),
        io_timeout: Duration::from_secs(2),
        tls_roots: Arc::new(roots),
    };
    let runtime = NetworkBroker::new(config)
        .unwrap()
        .runtime(identity())
        .unwrap();
    let endpoint = BrokerEndpoint::new(EndpointHost::Ip(IpAddr::V4(ip)), port).unwrap();
    let opened = dispatch(
        &runtime,
        "tls-open",
        BrokerOperation::TlsConnect {
            endpoint,
            profile: TlsProfile::new(
                "fixture.test".parse().unwrap(),
                Vec::new(),
                TlsVersion::Tls13,
            )
            .unwrap(),
        },
    );
    let BrokerWireResult::Opened { handle_id } = opened.result() else {
        panic!("unexpected TLS result: {:?}", opened.result());
    };
    assert_eq!(
        dispatch(
            &runtime,
            "tls-send",
            BrokerOperation::StreamSend {
                stream_id: *handle_id,
                payload: b"ping".to_vec(),
            },
        )
        .status(),
        BrokerWireStatus::Ok
    );
    assert_eq!(
        dispatch(
            &runtime,
            "tls-receive",
            BrokerOperation::StreamReceive {
                stream_id: *handle_id,
                max_bytes: 4,
            },
        )
        .result(),
        &BrokerWireResult::Received {
            payload: b"pong".to_vec(),
            binary: true,
        }
    );
    server.join().unwrap();
}

#[test]
fn deny_all_rejects_before_connecting() {
    let ip = host_ip();
    let endpoint = BrokerEndpoint::new(EndpointHost::Ip(IpAddr::V4(ip)), 9).unwrap();
    let runtime = NetworkBroker::denied(limits(), IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)))
        .unwrap()
        .runtime(identity())
        .unwrap();
    let response = dispatch(&runtime, "denied", BrokerOperation::TcpConnect { endpoint });

    assert_eq!(
        (response.status(), response.code()),
        (BrokerWireStatus::Denied, Some("policy_denied"))
    );
}

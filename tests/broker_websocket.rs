// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use hyperlight_unikraft::broker::{
    BrokerEndpoint, BrokerLimits, BrokerOperation, BrokerPolicy, BrokerProtocol, BrokerRequest,
    BrokerRequestId, DnsPolicy, EgressRule, EndpointHost, HostRule, PortRange, RequestIdentity,
    WebSocketProfile,
};
use hyperlight_unikraft::broker_adapter::{BrokerAuditAction, BrokerAuditOutcome};
use hyperlight_unikraft::broker_network::{NetworkBroker, NetworkBrokerConfig};
use hyperlight_unikraft::broker_wire::{
    BrokerWireResult, BrokerWireStatus, decode_response, encode_request,
};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::net::{IpAddr, Ipv4Addr, TcpListener, UdpSocket};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tungstenite::Message;

fn host_ip() -> Ipv4Addr {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect("8.8.8.8:80").unwrap();
    match socket.local_addr().unwrap().ip() {
        IpAddr::V4(ip) => ip,
        IpAddr::V6(ip) => panic!("expected IPv4 test host, got {ip}"),
    }
}

fn limits(max_message_bytes: u64) -> BrokerLimits {
    BrokerLimits {
        max_connections: 1,
        max_sockets: 1,
        max_datagrams: 1,
        max_datagram_bytes: 1024,
        max_stream_bytes: 1024,
        max_messages: 2,
        max_message_bytes,
        max_bytes: 2048,
        max_elapsed: Duration::from_secs(10),
        max_operations: 12,
        max_operations_per_window: 12,
        rate_window: Duration::from_secs(1),
        max_concurrency: 1,
    }
}

fn identity() -> RequestIdentity {
    RequestIdentity::new("websocket-fixture", "snapshot-a", 0).unwrap()
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

fn runtime(
    ip: Ipv4Addr,
    port: u16,
    limits: BrokerLimits,
) -> hyperlight_unikraft::broker_runtime::BrokerRuntime {
    let policy = BrokerPolicy::new(
        vec![
            EgressRule::new(
                HostRule::Ip(IpAddr::V4(ip)),
                vec![PortRange::new(port, port).unwrap()],
                vec![BrokerProtocol::WebSocket],
            )
            .unwrap(),
        ],
        DnsPolicy::Deny,
    );
    NetworkBroker::new(NetworkBrokerConfig {
        policy,
        limits,
        resolver: IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
        connect_timeout: Duration::from_secs(2),
        io_timeout: Duration::from_secs(2),
        tls_roots: Arc::new(rustls::RootCertStore::empty()),
    })
    .unwrap()
    .runtime(identity())
    .unwrap()
}

#[test]
fn websocket_is_policy_bound_bounded_audited_and_reset() {
    let ip = host_ip();
    let listener = TcpListener::bind((ip, 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_thread = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut socket = tungstenite::accept(stream).unwrap();
        assert_eq!(socket.read().unwrap(), Message::Text("ping".into()));
        socket
            .send(Message::Binary(b"pong".to_vec().into()))
            .unwrap();
    });
    let runtime = runtime(ip, port, limits(4));
    let endpoint = BrokerEndpoint::new(EndpointHost::Ip(IpAddr::V4(ip)), port).unwrap();
    let opened = dispatch(
        &runtime,
        "ws-open",
        BrokerOperation::WebSocketOpen {
            endpoint,
            secure: false,
            profile: WebSocketProfile::new(Vec::new()).unwrap(),
        },
    );
    let BrokerWireResult::Opened { handle_id } = opened.result() else {
        panic!("unexpected WebSocket open result: {:?}", opened.result());
    };
    assert_eq!(
        dispatch(
            &runtime,
            "ws-send",
            BrokerOperation::WebSocketSend {
                socket_id: *handle_id,
                payload: b"ping".to_vec(),
                binary: false,
            },
        )
        .result(),
        &BrokerWireResult::Transferred { bytes: 4 }
    );
    assert_eq!(
        dispatch(
            &runtime,
            "ws-receive",
            BrokerOperation::WebSocketReceive {
                socket_id: *handle_id,
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
            (BrokerAuditAction::WebSocketOpen, BrokerAuditOutcome::Ok),
            (BrokerAuditAction::WebSocketSend, BrokerAuditOutcome::Ok),
            (BrokerAuditAction::WebSocketReceive, BrokerAuditOutcome::Ok),
        ]
    );
    runtime.reset_for_fresh_vm().unwrap();
    let rejected = dispatch(
        &runtime,
        "ws-after-reset",
        BrokerOperation::WebSocketSend {
            socket_id: *handle_id,
            payload: b"x".to_vec(),
            binary: true,
        },
    );
    assert_eq!(
        (rejected.status(), rejected.code()),
        (
            BrokerWireStatus::InvalidRequest,
            Some("invalid_websocket_handle")
        )
    );
    println!(
        "DEMO_EVIDENCE={}",
        serde_json::json!({
            "demo": "websocket",
            "opened": true,
            "sent": {"type": "text", "bytes": 4},
            "received": {"type": "binary", "body": "pong"},
            "audit_events": 3,
            "after_reset": "invalid_websocket_handle",
        })
    );
    server_thread.join().unwrap();
}

#[test]
fn websocket_message_limit_rejects_before_host_send() {
    let ip = host_ip();
    let listener = TcpListener::bind((ip, 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_thread = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let _socket = tungstenite::accept(stream).unwrap();
    });
    let runtime = runtime(ip, port, limits(3));
    let endpoint = BrokerEndpoint::new(EndpointHost::Ip(IpAddr::V4(ip)), port).unwrap();
    let opened = dispatch(
        &runtime,
        "ws-open",
        BrokerOperation::WebSocketOpen {
            endpoint,
            secure: false,
            profile: WebSocketProfile::new(Vec::new()).unwrap(),
        },
    );
    let BrokerWireResult::Opened { handle_id } = opened.result() else {
        panic!("unexpected WebSocket open result: {:?}", opened.result());
    };
    let rejected = dispatch(
        &runtime,
        "ws-too-large",
        BrokerOperation::WebSocketSend {
            socket_id: *handle_id,
            payload: b"four".to_vec(),
            binary: true,
        },
    );
    assert_eq!(
        (rejected.status(), rejected.code()),
        (BrokerWireStatus::QuotaExceeded, Some("message_size_quota"))
    );
    println!(
        "DEMO_EVIDENCE={}",
        serde_json::json!({
            "demo": "websocket-limit",
            "status": "quota_exceeded",
            "code": "message_size_quota",
            "host_send": false,
        })
    );
    server_thread.join().unwrap();
}

#[test]
fn secure_websocket_is_host_terminated_with_host_owned_roots() {
    let ip = host_ip();
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec![ip.to_string()]).unwrap();
    let certificate: CertificateDer<'static> = cert.der().clone();
    let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], private_key)
        .unwrap();
    let listener = TcpListener::bind((ip, 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_thread = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let connection = rustls::ServerConnection::new(Arc::new(server_config)).unwrap();
        let stream = rustls::StreamOwned::new(connection, stream);
        let mut socket = tungstenite::accept(stream).unwrap();
        assert_eq!(
            socket.read().unwrap(),
            Message::Binary(b"ping".to_vec().into())
        );
        socket.send(Message::Text("pong".into())).unwrap();
    });
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).unwrap();
    let policy = BrokerPolicy::new(
        vec![
            EgressRule::new(
                HostRule::Ip(IpAddr::V4(ip)),
                vec![PortRange::new(port, port).unwrap()],
                vec![BrokerProtocol::WebSocketSecure],
            )
            .unwrap(),
        ],
        DnsPolicy::Deny,
    );
    let runtime = NetworkBroker::new(NetworkBrokerConfig {
        policy,
        limits: limits(4),
        resolver: IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
        connect_timeout: Duration::from_secs(2),
        io_timeout: Duration::from_secs(2),
        tls_roots: Arc::new(roots),
    })
    .unwrap()
    .runtime(identity())
    .unwrap();
    let endpoint = BrokerEndpoint::new(EndpointHost::Ip(IpAddr::V4(ip)), port).unwrap();
    let opened = dispatch(
        &runtime,
        "wss-open",
        BrokerOperation::WebSocketOpen {
            endpoint,
            secure: true,
            profile: WebSocketProfile::new(Vec::new()).unwrap(),
        },
    );
    let BrokerWireResult::Opened { handle_id } = opened.result() else {
        panic!("unexpected secure WebSocket result: {:?}", opened.result());
    };
    assert_eq!(
        dispatch(
            &runtime,
            "wss-send",
            BrokerOperation::WebSocketSend {
                socket_id: *handle_id,
                payload: b"ping".to_vec(),
                binary: true,
            },
        )
        .status(),
        BrokerWireStatus::Ok
    );
    assert_eq!(
        dispatch(
            &runtime,
            "wss-receive",
            BrokerOperation::WebSocketReceive {
                socket_id: *handle_id,
                max_bytes: 4,
            },
        )
        .result(),
        &BrokerWireResult::Received {
            payload: b"pong".to_vec(),
            binary: false,
        }
    );
    println!(
        "DEMO_EVIDENCE={}",
        serde_json::json!({
            "demo": "websocket-secure",
            "opened": true,
            "tls": "host-terminated",
            "trust_roots": "host-owned",
            "received": "pong",
        })
    );
    server_thread.join().unwrap();
}

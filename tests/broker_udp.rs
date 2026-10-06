// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use hyperlight_unikraft::broker::{
    BrokerEndpoint, BrokerLimits, BrokerOperation, BrokerPolicy, BrokerProtocol, BrokerRequest,
    BrokerRequestId, DnsPolicy, EgressRule, EndpointHost, HostRule, PortRange, RequestIdentity,
};
use hyperlight_unikraft::broker_adapter::{BrokerAuditAction, BrokerAuditOutcome};
use hyperlight_unikraft::broker_network::{NetworkBroker, NetworkBrokerConfig};
use hyperlight_unikraft::broker_wire::{
    BrokerWireResult, BrokerWireStatus, decode_response, encode_request,
};
use std::net::{IpAddr, Ipv4Addr, UdpSocket};
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

fn limits(max_datagram_bytes: u64) -> BrokerLimits {
    BrokerLimits {
        max_connections: 1,
        max_sockets: 1,
        max_datagrams: 2,
        max_datagram_bytes,
        max_stream_bytes: 1024,
        max_messages: 1,
        max_message_bytes: 1024,
        max_bytes: 2048,
        max_elapsed: Duration::from_secs(10),
        max_operations: 12,
        max_operations_per_window: 12,
        rate_window: Duration::from_secs(1),
        max_concurrency: 1,
    }
}

fn identity() -> RequestIdentity {
    RequestIdentity::new("udp-fixture", "snapshot-a", 0).unwrap()
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
                vec![BrokerProtocol::Udp],
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
fn connected_udp_is_policy_bound_bounded_audited_and_reset() {
    let ip = host_ip();
    let server = UdpSocket::bind((ip, 0)).unwrap();
    let port = server.local_addr().unwrap().port();
    let server_thread = thread::spawn(move || {
        let mut payload = [0; 4];
        let (bytes, peer) = server.recv_from(&mut payload).unwrap();
        assert_eq!(&payload[..bytes], b"ping");
        server.send_to(b"pong", peer).unwrap();
    });
    let runtime = runtime(ip, port, limits(4));
    let endpoint = BrokerEndpoint::new(EndpointHost::Ip(IpAddr::V4(ip)), port).unwrap();
    let opened = dispatch(&runtime, "udp-open", BrokerOperation::UdpOpen { endpoint });
    let BrokerWireResult::Opened { handle_id } = opened.result() else {
        panic!("unexpected UDP open result: {:?}", opened.result());
    };
    assert_eq!(
        dispatch(
            &runtime,
            "udp-send",
            BrokerOperation::UdpSocketSend {
                socket_id: *handle_id,
                payload: b"ping".to_vec(),
            },
        )
        .result(),
        &BrokerWireResult::Transferred { bytes: 4 }
    );
    assert_eq!(
        dispatch(
            &runtime,
            "udp-receive",
            BrokerOperation::UdpReceive {
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
            (BrokerAuditAction::UdpOpen, BrokerAuditOutcome::Ok),
            (BrokerAuditAction::UdpSend, BrokerAuditOutcome::Ok),
            (BrokerAuditAction::UdpReceive, BrokerAuditOutcome::Ok),
        ]
    );
    runtime.reset_for_fresh_vm().unwrap();
    let rejected = dispatch(
        &runtime,
        "udp-after-reset",
        BrokerOperation::UdpSocketSend {
            socket_id: *handle_id,
            payload: b"x".to_vec(),
        },
    );
    assert_eq!(
        (rejected.status(), rejected.code()),
        (BrokerWireStatus::InvalidRequest, Some("invalid_udp_handle"))
    );
    println!(
        r#"DEMO_EVIDENCE={{"demo":"udp","opened":true,"sent_bytes":4,"received":"pong","audit_events":3,"after_reset":"invalid_udp_handle"}}"#
    );
    server_thread.join().unwrap();
}

#[test]
fn udp_datagram_limit_rejects_before_host_send() {
    let ip = host_ip();
    let server = UdpSocket::bind((ip, 0)).unwrap();
    let port = server.local_addr().unwrap().port();
    let runtime = runtime(ip, port, limits(3));
    let endpoint = BrokerEndpoint::new(EndpointHost::Ip(IpAddr::V4(ip)), port).unwrap();
    let response = dispatch(
        &runtime,
        "udp-too-large",
        BrokerOperation::UdpSend {
            endpoint,
            payload: b"four".to_vec(),
        },
    );

    assert_eq!(
        (response.status(), response.code()),
        (BrokerWireStatus::QuotaExceeded, Some("datagram_size_quota"))
    );
    println!(
        r#"DEMO_EVIDENCE={{"demo":"udp-limit","status":"quota_exceeded","code":"datagram_size_quota","host_send":false}}"#
    );
}

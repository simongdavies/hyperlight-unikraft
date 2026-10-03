//! Explicit versioned binary wire schema for guest-to-host broker requests.
//!
//! Rust enum representation is never used on the wire. Every tag, integer
//! width, byte order, and length prefix is fixed here and covered by fixtures.

use crate::broker::{
    BrokerEndpoint, BrokerOperation, BrokerRequest, BrokerRequestId, DnsName, EndpointHost,
    TlsProfile, TlsVersion, WebSocketProfile,
};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

const MAGIC: &[u8; 4] = b"HLBR";
const MESSAGE_REQUEST: u8 = 1;
const MESSAGE_RESPONSE: u8 = 2;
const MAX_WIRE_BYTES: usize = 1024 * 1024;
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024 - 4096;
const MAX_STRING_BYTES: usize = 1024;

/// Current bounded broker wire version.
pub const BROKER_WIRE_VERSION: u16 = 1;

/// Non-leaking guest-visible result status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerWireStatus {
    Ok,
    Denied,
    QuotaExceeded,
    InvalidRequest,
    HostError,
}

/// Guest-visible result body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerWireResult {
    None,
    Opened { handle_id: u64 },
    Transferred { bytes: u64 },
    Closed,
}

/// Versioned response returned to the guest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerWireResponse {
    request_id: Option<BrokerRequestId>,
    status: BrokerWireStatus,
    result: BrokerWireResult,
    code: Option<String>,
}

impl BrokerWireResponse {
    /// Construct a response. `code` is a stable category, never a raw host error.
    pub fn new(
        request_id: Option<BrokerRequestId>,
        status: BrokerWireStatus,
        result: BrokerWireResult,
        code: Option<String>,
    ) -> Result<Self, BrokerWireError> {
        if code
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 128 || !value.is_ascii())
        {
            return Err(BrokerWireError::InvalidField("response code"));
        }
        Ok(Self {
            request_id,
            status,
            result,
            code,
        })
    }

    /// Guest correlation ID, absent when the request could not be decoded.
    pub fn request_id(&self) -> Option<&BrokerRequestId> {
        self.request_id.as_ref()
    }

    /// Non-leaking result status.
    pub fn status(&self) -> BrokerWireStatus {
        self.status
    }

    /// Typed response body.
    pub fn result(&self) -> BrokerWireResult {
        self.result
    }

    /// Stable error category suitable for metrics and guest branching.
    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }
}

/// Encode one v1 guest request.
pub fn encode_request(request: &BrokerRequest) -> Result<Vec<u8>, BrokerWireError> {
    let mut output = Writer::new(MESSAGE_REQUEST);
    output.string(request.request_id().as_str())?;
    match request.operation() {
        BrokerOperation::TcpConnect { endpoint } => {
            output.u8(1);
            output.endpoint(endpoint)?;
        }
        BrokerOperation::TlsConnect { endpoint, profile } => {
            output.u8(2);
            output.endpoint(endpoint)?;
            output.string(profile.server_name().as_str())?;
            output.u8(match profile.minimum_version() {
                TlsVersion::Tls12 => 12,
                TlsVersion::Tls13 => 13,
            });
            output.count(profile.alpn().len())?;
            for protocol in profile.alpn() {
                output.string(protocol)?;
            }
        }
        BrokerOperation::UdpSend { endpoint, payload } => {
            output.u8(3);
            output.endpoint(endpoint)?;
            output.bytes(payload)?;
        }
        BrokerOperation::WebSocketOpen {
            endpoint,
            secure,
            profile,
        } => {
            output.u8(4);
            output.endpoint(endpoint)?;
            output.u8(u8::from(*secure));
            output.count(profile.subprotocols().len())?;
            for protocol in profile.subprotocols() {
                output.string(protocol)?;
            }
        }
        BrokerOperation::WebSocketSend {
            socket_id,
            payload,
            binary,
        } => {
            output.u8(5);
            output.u64(*socket_id);
            output.u8(u8::from(*binary));
            output.bytes(payload)?;
        }
        BrokerOperation::Close { handle_id } => {
            output.u8(6);
            output.u64(*handle_id);
        }
    }
    output.finish()
}

/// Decode one v1 guest request with strict tag and trailing-byte rejection.
pub fn decode_request(input: &[u8]) -> Result<BrokerRequest, BrokerWireError> {
    let mut reader = Reader::new(input, MESSAGE_REQUEST)?;
    let request_id = BrokerRequestId::new(reader.string()?)
        .map_err(|_| BrokerWireError::InvalidField("request_id"))?;
    let operation = match reader.u8()? {
        1 => BrokerOperation::TcpConnect {
            endpoint: reader.endpoint()?,
        },
        2 => {
            let endpoint = reader.endpoint()?;
            let server_name = DnsName::from_str(&reader.string()?)
                .map_err(|_| BrokerWireError::InvalidField("TLS server name"))?;
            let minimum_version = match reader.u8()? {
                12 => TlsVersion::Tls12,
                13 => TlsVersion::Tls13,
                value => return Err(BrokerWireError::UnknownTag("TLS version", value)),
            };
            let alpn = reader.strings()?;
            let profile = TlsProfile::new(server_name, alpn, minimum_version)
                .map_err(|_| BrokerWireError::InvalidField("TLS profile"))?;
            BrokerOperation::TlsConnect { endpoint, profile }
        }
        3 => BrokerOperation::UdpSend {
            endpoint: reader.endpoint()?,
            payload: reader.bytes()?,
        },
        4 => {
            let endpoint = reader.endpoint()?;
            let secure = reader.bool()?;
            let profile = WebSocketProfile::new(reader.strings()?)
                .map_err(|_| BrokerWireError::InvalidField("WebSocket profile"))?;
            BrokerOperation::WebSocketOpen {
                endpoint,
                secure,
                profile,
            }
        }
        5 => BrokerOperation::WebSocketSend {
            socket_id: reader.u64()?,
            binary: reader.bool()?,
            payload: reader.bytes()?,
        },
        6 => BrokerOperation::Close {
            handle_id: reader.u64()?,
        },
        value => return Err(BrokerWireError::UnknownTag("operation", value)),
    };
    reader.finish()?;
    Ok(BrokerRequest::new(request_id, operation))
}

/// Encode one v1 guest response.
pub fn encode_response(response: &BrokerWireResponse) -> Result<Vec<u8>, BrokerWireError> {
    let mut output = Writer::new(MESSAGE_RESPONSE);
    output.string(
        response
            .request_id()
            .map(BrokerRequestId::as_str)
            .unwrap_or(""),
    )?;
    output.u8(match response.status() {
        BrokerWireStatus::Ok => 0,
        BrokerWireStatus::Denied => 1,
        BrokerWireStatus::QuotaExceeded => 2,
        BrokerWireStatus::InvalidRequest => 3,
        BrokerWireStatus::HostError => 4,
    });
    match response.result() {
        BrokerWireResult::None => output.u8(0),
        BrokerWireResult::Opened { handle_id } => {
            output.u8(1);
            output.u64(handle_id);
        }
        BrokerWireResult::Transferred { bytes } => {
            output.u8(2);
            output.u64(bytes);
        }
        BrokerWireResult::Closed => output.u8(3),
    }
    output.string(response.code().unwrap_or(""))?;
    output.finish()
}

/// Decode one v1 guest response.
pub fn decode_response(input: &[u8]) -> Result<BrokerWireResponse, BrokerWireError> {
    let mut reader = Reader::new(input, MESSAGE_RESPONSE)?;
    let request_id = match reader.string()? {
        value if value.is_empty() => None,
        value => Some(
            BrokerRequestId::new(value).map_err(|_| BrokerWireError::InvalidField("request_id"))?,
        ),
    };
    let status = match reader.u8()? {
        0 => BrokerWireStatus::Ok,
        1 => BrokerWireStatus::Denied,
        2 => BrokerWireStatus::QuotaExceeded,
        3 => BrokerWireStatus::InvalidRequest,
        4 => BrokerWireStatus::HostError,
        value => return Err(BrokerWireError::UnknownTag("response status", value)),
    };
    let result = match reader.u8()? {
        0 => BrokerWireResult::None,
        1 => BrokerWireResult::Opened {
            handle_id: reader.u64()?,
        },
        2 => BrokerWireResult::Transferred {
            bytes: reader.u64()?,
        },
        3 => BrokerWireResult::Closed,
        value => return Err(BrokerWireError::UnknownTag("response result", value)),
    };
    let code = match reader.string()? {
        value if value.is_empty() => None,
        value => Some(value),
    };
    reader.finish()?;
    BrokerWireResponse::new(request_id, status, result, code)
}

struct Writer {
    output: Vec<u8>,
}

impl Writer {
    fn new(message_type: u8) -> Self {
        let mut output = Vec::with_capacity(128);
        output.extend_from_slice(MAGIC);
        output.extend_from_slice(&BROKER_WIRE_VERSION.to_be_bytes());
        output.push(message_type);
        Self { output }
    }

    fn u8(&mut self, value: u8) {
        self.output.push(value);
    }

    fn u16(&mut self, value: u16) {
        self.output.extend_from_slice(&value.to_be_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.output.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.output.extend_from_slice(&value.to_be_bytes());
    }

    fn count(&mut self, value: usize) -> Result<(), BrokerWireError> {
        let value = u8::try_from(value).map_err(|_| BrokerWireError::TooLarge)?;
        self.u8(value);
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<(), BrokerWireError> {
        if value.len() > MAX_STRING_BYTES {
            return Err(BrokerWireError::TooLarge);
        }
        let length = u16::try_from(value.len()).map_err(|_| BrokerWireError::TooLarge)?;
        self.u16(length);
        self.output.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), BrokerWireError> {
        if value.len() > MAX_PAYLOAD_BYTES {
            return Err(BrokerWireError::TooLarge);
        }
        let length = u32::try_from(value.len()).map_err(|_| BrokerWireError::TooLarge)?;
        self.u32(length);
        self.output.extend_from_slice(value);
        Ok(())
    }

    fn endpoint(&mut self, endpoint: &BrokerEndpoint) -> Result<(), BrokerWireError> {
        match endpoint.host() {
            EndpointHost::Ip(IpAddr::V4(ip)) => {
                self.u8(1);
                self.output.extend_from_slice(&ip.octets());
            }
            EndpointHost::Ip(IpAddr::V6(ip)) => {
                self.u8(2);
                self.output.extend_from_slice(&ip.octets());
            }
            EndpointHost::Dns(name) => {
                self.u8(3);
                self.string(name.as_str())?;
            }
        }
        self.u16(endpoint.port());
        Ok(())
    }

    fn finish(self) -> Result<Vec<u8>, BrokerWireError> {
        if self.output.len() > MAX_WIRE_BYTES {
            Err(BrokerWireError::TooLarge)
        } else {
            Ok(self.output)
        }
    }
}

struct Reader<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(input: &'a [u8], expected_type: u8) -> Result<Self, BrokerWireError> {
        if input.len() > MAX_WIRE_BYTES {
            return Err(BrokerWireError::TooLarge);
        }
        let mut reader = Self { input, position: 0 };
        if reader.take(4)? != MAGIC {
            return Err(BrokerWireError::InvalidMagic);
        }
        let version = reader.u16()?;
        if version != BROKER_WIRE_VERSION {
            return Err(BrokerWireError::UnsupportedVersion(version));
        }
        let actual_type = reader.u8()?;
        if actual_type != expected_type {
            return Err(BrokerWireError::WrongMessageType {
                expected: expected_type,
                actual: actual_type,
            });
        }
        Ok(reader)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], BrokerWireError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(BrokerWireError::Truncated)?;
        let value = self
            .input
            .get(self.position..end)
            .ok_or(BrokerWireError::Truncated)?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, BrokerWireError> {
        Ok(self.take(1)?[0])
    }

    fn bool(&mut self) -> Result<bool, BrokerWireError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(BrokerWireError::UnknownTag("boolean", value)),
        }
    }

    fn u16(&mut self) -> Result<u16, BrokerWireError> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u32(&mut self) -> Result<u32, BrokerWireError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> Result<u64, BrokerWireError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn string(&mut self) -> Result<String, BrokerWireError> {
        let length = usize::from(self.u16()?);
        if length > MAX_STRING_BYTES {
            return Err(BrokerWireError::TooLarge);
        }
        let value =
            std::str::from_utf8(self.take(length)?).map_err(|_| BrokerWireError::InvalidUtf8)?;
        Ok(value.to_string())
    }

    fn strings(&mut self) -> Result<Vec<String>, BrokerWireError> {
        let count = usize::from(self.u8()?);
        (0..count).map(|_| self.string()).collect()
    }

    fn bytes(&mut self) -> Result<Vec<u8>, BrokerWireError> {
        let length = usize::try_from(self.u32()?).map_err(|_| BrokerWireError::TooLarge)?;
        if length > MAX_PAYLOAD_BYTES {
            return Err(BrokerWireError::TooLarge);
        }
        Ok(self.take(length)?.to_vec())
    }

    fn endpoint(&mut self) -> Result<BrokerEndpoint, BrokerWireError> {
        let host = match self.u8()? {
            1 => {
                let bytes = self.take(4)?;
                EndpointHost::Ip(IpAddr::V4(Ipv4Addr::new(
                    bytes[0], bytes[1], bytes[2], bytes[3],
                )))
            }
            2 => {
                let bytes = self.take(16)?;
                let mut octets = [0u8; 16];
                octets.copy_from_slice(bytes);
                EndpointHost::Ip(IpAddr::V6(Ipv6Addr::from(octets)))
            }
            3 => EndpointHost::Dns(
                DnsName::from_str(&self.string()?)
                    .map_err(|_| BrokerWireError::InvalidField("endpoint DNS name"))?,
            ),
            value => return Err(BrokerWireError::UnknownTag("endpoint host", value)),
        };
        BrokerEndpoint::new(host, self.u16()?)
            .map_err(|_| BrokerWireError::InvalidField("endpoint port"))
    }

    fn finish(self) -> Result<(), BrokerWireError> {
        if self.position == self.input.len() {
            Ok(())
        } else {
            Err(BrokerWireError::TrailingBytes)
        }
    }
}

/// Strict wire parsing or encoding error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrokerWireError {
    InvalidMagic,
    UnsupportedVersion(u16),
    WrongMessageType { expected: u8, actual: u8 },
    Truncated,
    InvalidUtf8,
    InvalidField(&'static str),
    UnknownTag(&'static str, u8),
    TrailingBytes,
    TooLarge,
}

impl fmt::Display for BrokerWireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "broker wire error: {:?}", self)
    }
}

impl std::error::Error for BrokerWireError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::BrokerRequestId;

    const TCP_FIXTURE_HEX: &str = include_str!("../tests/fixtures/broker_wire_v1_tcp.hex");

    fn decode_hex(value: &str) -> Vec<u8> {
        let value = value.trim();
        let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
        assert!(remainder.is_empty(), "hex fixture must contain whole bytes");
        pairs
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    fn tcp_request() -> BrokerRequest {
        BrokerRequest::new(
            BrokerRequestId::new("req-1").unwrap(),
            BrokerOperation::TcpConnect {
                endpoint: BrokerEndpoint::new(
                    EndpointHost::Dns("api.example.com".parse().unwrap()),
                    443,
                )
                .unwrap(),
            },
        )
    }

    #[test]
    fn tcp_fixture_fixes_v1_bytes() {
        assert_eq!(
            encode_request(&tcp_request()).unwrap(),
            decode_hex(TCP_FIXTURE_HEX)
        );
    }

    #[test]
    fn all_request_variants_roundtrip() {
        let endpoint =
            BrokerEndpoint::new(EndpointHost::Dns("api.example.com".parse().unwrap()), 443)
                .unwrap();
        let requests = vec![
            tcp_request(),
            BrokerRequest::new(
                BrokerRequestId::new("tls").unwrap(),
                BrokerOperation::TlsConnect {
                    endpoint: endpoint.clone(),
                    profile: TlsProfile::new(
                        "api.example.com".parse().unwrap(),
                        vec!["h2".to_string()],
                        TlsVersion::Tls13,
                    )
                    .unwrap(),
                },
            ),
            BrokerRequest::new(
                BrokerRequestId::new("udp").unwrap(),
                BrokerOperation::UdpSend {
                    endpoint: endpoint.clone(),
                    payload: vec![1, 2, 3],
                },
            ),
            BrokerRequest::new(
                BrokerRequestId::new("ws-open").unwrap(),
                BrokerOperation::WebSocketOpen {
                    endpoint,
                    secure: true,
                    profile: WebSocketProfile::new(vec!["chat".to_string()]).unwrap(),
                },
            ),
            BrokerRequest::new(
                BrokerRequestId::new("ws-send").unwrap(),
                BrokerOperation::WebSocketSend {
                    socket_id: 7,
                    payload: vec![4, 5],
                    binary: true,
                },
            ),
            BrokerRequest::new(
                BrokerRequestId::new("close").unwrap(),
                BrokerOperation::Close { handle_id: 7 },
            ),
        ];

        assert_eq!(
            requests
                .iter()
                .map(|request| decode_request(&encode_request(request).unwrap()).unwrap())
                .collect::<Vec<_>>(),
            requests
        );
    }

    #[test]
    fn response_roundtrips_without_host_error_details() {
        let response = BrokerWireResponse::new(
            Some(BrokerRequestId::new("req-1").unwrap()),
            BrokerWireStatus::Denied,
            BrokerWireResult::None,
            Some("policy_denied".to_string()),
        )
        .unwrap();

        assert_eq!(
            decode_response(&encode_response(&response).unwrap()).unwrap(),
            response
        );
    }

    #[test]
    fn decoder_rejects_wrong_version_and_trailing_bytes() {
        let mut wrong_version = encode_request(&tcp_request()).unwrap();
        wrong_version[5] = 2;
        let mut trailing = encode_request(&tcp_request()).unwrap();
        trailing.push(0);

        assert_eq!(
            (decode_request(&wrong_version), decode_request(&trailing),),
            (
                Err(BrokerWireError::UnsupportedVersion(2)),
                Err(BrokerWireError::TrailingBytes),
            )
        );
    }
}

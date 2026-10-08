// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{Error, Result};
use crate::broker::{
    BrokerLimits, BrokerPolicy, BrokerProtocol, DnsPolicy, EgressRule, HostRule, PortRange,
};
use crate::broker_network::{NetworkBroker, NetworkBrokerConfig, default_tls_roots};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawEgressRule {
    pub host: String,
    pub ports: Vec<u16>,
    pub protocols: Vec<BrokerProtocol>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawNetworkPolicyConfig {
    pub rules: Vec<RawEgressRule>,
    pub dns_names: Vec<String>,
    pub resolver_identity: IpAddr,
    pub ip_ranges: Vec<ipnet::IpNet>,
    #[serde(default)]
    pub allow_loopback: bool,
    #[serde(default)]
    pub allow_private: bool,
    #[serde(default)]
    pub allow_metadata: bool,
    pub limits: BrokerLimits,
    pub connect_timeout_ms: u64,
    pub io_timeout_ms: u64,
}

impl RawNetworkPolicyConfig {
    pub(super) fn broker(&self) -> Result<NetworkBroker> {
        let rules = self
            .rules
            .iter()
            .map(|rule| {
                let host = if let Ok(ip) = rule.host.parse::<IpAddr>() {
                    HostRule::Ip(ip)
                } else {
                    HostRule::ExactDns(rule.host.parse().map_err(
                        |error: crate::broker::BrokerContractError| Error::State(error.to_string()),
                    )?)
                };
                let ports = rule
                    .ports
                    .iter()
                    .map(|port| PortRange::new(*port, *port))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|error| Error::State(error.to_string()))?;
                EgressRule::new(host, ports, rule.protocols.clone())
                    .map_err(|error| Error::State(error.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let names = self
            .dns_names
            .iter()
            .map(|name| name.parse())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error: crate::broker::BrokerContractError| Error::State(error.to_string()))?;
        let dns = if self.dns_names.is_empty() {
            DnsPolicy::Deny
        } else {
            DnsPolicy::Allow {
                names,
                resolvers: vec![self.resolver_identity],
            }
        };
        let policy = BrokerPolicy::new(rules, dns)
            .with_address_policy(
                self.ip_ranges.clone(),
                self.allow_loopback,
                self.allow_private,
                self.allow_metadata,
            )
            .map_err(|error| Error::State(error.to_string()))?;
        NetworkBroker::new(NetworkBrokerConfig {
            policy,
            limits: self.limits,
            resolver: self.resolver_identity,
            connect_timeout: Duration::from_millis(self.connect_timeout_ms),
            io_timeout: Duration::from_millis(self.io_timeout_ms),
            tls_roots: Arc::new(default_tls_roots()),
        })
        .map_err(|error| Error::State(error.to_string()))
    }
}

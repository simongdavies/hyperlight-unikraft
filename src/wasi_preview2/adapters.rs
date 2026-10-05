use super::policy::{CliPolicy, ClockPolicy, RandomPolicy};
use crate::broker_runtime::BrokerRuntime;
use crate::workerd::{FetchBroker, RequestEnvelope, ResponseEnvelope};
use crate::{Mount, MountLimits};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdapterError {
    Denied(&'static str),
    InvalidArgument(&'static str),
    QuotaExceeded(&'static str),
    Closed,
    NotSupported(&'static str),
    HostFailure,
}

impl AdapterError {
    pub fn stable_code(&self) -> &'static str {
        match self {
            Self::Denied(_) => "capability-denied",
            Self::InvalidArgument(_) => "invalid-argument",
            Self::QuotaExceeded(_) => "quota-exceeded",
            Self::Closed => "invalid-handle",
            Self::NotSupported(_) => "not-supported",
            Self::HostFailure => "host-failure",
        }
    }
}

impl fmt::Display for AdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.stable_code())
    }
}

impl std::error::Error for AdapterError {}

#[derive(Clone, Debug)]
pub struct InputStream {
    bytes: VecDeque<u8>,
    max_buffer_bytes: usize,
    max_chunk_bytes: usize,
    closed: bool,
}

impl InputStream {
    pub fn new(
        bytes: impl IntoIterator<Item = u8>,
        max_buffer_bytes: usize,
        max_chunk_bytes: usize,
    ) -> Result<Self, AdapterError> {
        if max_buffer_bytes == 0 || max_chunk_bytes == 0 || max_chunk_bytes > max_buffer_bytes {
            return Err(AdapterError::InvalidArgument("invalid input stream limits"));
        }
        let bytes: VecDeque<u8> = bytes.into_iter().collect();
        if bytes.len() > max_buffer_bytes {
            return Err(AdapterError::QuotaExceeded("input stream buffer"));
        }
        Ok(Self {
            bytes,
            max_buffer_bytes,
            max_chunk_bytes,
            closed: false,
        })
    }

    pub fn read(&mut self, requested: usize) -> Result<Vec<u8>, AdapterError> {
        if self.closed {
            return Err(AdapterError::Closed);
        }
        let count = requested.min(self.max_chunk_bytes).min(self.bytes.len());
        Ok(self.bytes.drain(..count).collect())
    }

    pub fn append(&mut self, bytes: &[u8]) -> Result<(), AdapterError> {
        if self.closed {
            return Err(AdapterError::Closed);
        }
        if self.bytes.len().saturating_add(bytes.len()) > self.max_buffer_bytes {
            return Err(AdapterError::QuotaExceeded("input stream buffer"));
        }
        self.bytes.extend(bytes);
        Ok(())
    }

    pub fn ready(&self) -> bool {
        self.closed || !self.bytes.is_empty()
    }

    pub fn close(&mut self) {
        self.closed = true;
    }
}

#[derive(Clone, Debug)]
pub struct OutputStream {
    bytes: Vec<u8>,
    max_bytes: usize,
    max_chunk_bytes: usize,
    closed: bool,
}

impl OutputStream {
    pub fn new(max_bytes: usize, max_chunk_bytes: usize) -> Result<Self, AdapterError> {
        if max_bytes == 0 || max_chunk_bytes == 0 || max_chunk_bytes > max_bytes {
            return Err(AdapterError::InvalidArgument(
                "invalid output stream limits",
            ));
        }
        Ok(Self {
            bytes: Vec::new(),
            max_bytes,
            max_chunk_bytes,
            closed: false,
        })
    }

    pub fn write(&mut self, bytes: &[u8]) -> Result<usize, AdapterError> {
        if self.closed {
            return Err(AdapterError::Closed);
        }
        let count = bytes.len().min(self.max_chunk_bytes);
        if self.bytes.len().saturating_add(count) > self.max_bytes {
            return Err(AdapterError::QuotaExceeded("output stream bytes"));
        }
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn close(&mut self) {
        self.closed = true;
    }
}

#[derive(Clone, Debug)]
pub struct PollSet {
    max_pollables: usize,
}

impl PollSet {
    pub fn new(max_pollables: usize) -> Result<Self, AdapterError> {
        if max_pollables == 0 {
            return Err(AdapterError::InvalidArgument(
                "pollable limit must be nonzero",
            ));
        }
        Ok(Self { max_pollables })
    }

    pub fn poll(&self, readiness: &[bool]) -> Result<Vec<usize>, AdapterError> {
        if readiness.len() > self.max_pollables {
            return Err(AdapterError::QuotaExceeded("pollable count"));
        }
        Ok(readiness
            .iter()
            .enumerate()
            .filter_map(|(index, ready)| ready.then_some(index))
            .collect())
    }
}

pub trait ClockBackend {
    fn wall_clock_ns(&self) -> Result<u64, AdapterError>;
    fn monotonic_clock_ns(&self) -> Result<u64, AdapterError>;
}

pub struct ClockAdapter<B> {
    policy: ClockPolicy,
    backend: B,
    deterministic_tick: u64,
}

impl<B: ClockBackend> ClockAdapter<B> {
    pub fn new(policy: ClockPolicy, backend: B) -> Self {
        Self {
            policy,
            backend,
            deterministic_tick: 0,
        }
    }

    pub fn wall_clock_ns(&self) -> Result<u64, AdapterError> {
        match &self.policy {
            ClockPolicy::Denied | ClockPolicy::Host { wall: false, .. } => {
                Err(AdapterError::Denied("wall clock"))
            }
            ClockPolicy::Deterministic { wall_epoch_ns, .. } => Ok(*wall_epoch_ns),
            ClockPolicy::Host { wall: true, .. } => self.backend.wall_clock_ns(),
        }
    }

    pub fn monotonic_clock_ns(&mut self) -> Result<u64, AdapterError> {
        match &self.policy {
            ClockPolicy::Denied
            | ClockPolicy::Host {
                monotonic: false, ..
            } => Err(AdapterError::Denied("monotonic clock")),
            ClockPolicy::Deterministic {
                monotonic_step_ns, ..
            } => {
                let value = self.deterministic_tick;
                self.deterministic_tick =
                    self.deterministic_tick
                        .checked_add(*monotonic_step_ns)
                        .ok_or(AdapterError::QuotaExceeded("monotonic clock range"))?;
                Ok(value)
            }
            ClockPolicy::Host {
                monotonic: true, ..
            } => self.backend.monotonic_clock_ns(),
        }
    }

    pub fn timezone(&self) -> Result<&str, AdapterError> {
        let timezone = match &self.policy {
            ClockPolicy::Deterministic { timezone, .. } => timezone.as_deref(),
            ClockPolicy::Host { fixed_timezone, .. } => fixed_timezone.as_deref(),
            ClockPolicy::Denied => None,
        };
        timezone.ok_or(AdapterError::Denied("timezone"))
    }
}

pub trait SecureRandom {
    fn fill_secure(&mut self, output: &mut [u8]) -> Result<(), AdapterError>;
}

pub struct RandomAdapter<R> {
    policy: RandomPolicy,
    secure: R,
    secure_bytes: u64,
    insecure_state: [u64; 4],
}

impl<R: SecureRandom> RandomAdapter<R> {
    pub fn new(policy: RandomPolicy, secure: R) -> Self {
        let seed = match &policy {
            RandomPolicy::SecureAndDeterministicInsecure { insecure_seed, .. } => *insecure_seed,
            _ => [0; 32],
        };
        let mut insecure_state = [
            u64::from_le_bytes(seed[0..8].try_into().expect("eight-byte seed word")),
            u64::from_le_bytes(seed[8..16].try_into().expect("eight-byte seed word")),
            u64::from_le_bytes(seed[16..24].try_into().expect("eight-byte seed word")),
            u64::from_le_bytes(seed[24..32].try_into().expect("eight-byte seed word")),
        ];
        if insecure_state == [0; 4] {
            insecure_state = [1, 2, 3, 4];
        }
        Self {
            policy,
            secure,
            secure_bytes: 0,
            insecure_state,
        }
    }

    pub fn secure_bytes(&mut self, count: usize) -> Result<Vec<u8>, AdapterError> {
        let limit = match &self.policy {
            RandomPolicy::Denied => return Err(AdapterError::Denied("secure random")),
            RandomPolicy::Secure { max_bytes } => *max_bytes,
            RandomPolicy::SecureAndDeterministicInsecure {
                max_secure_bytes, ..
            } => *max_secure_bytes,
        };
        let next = self
            .secure_bytes
            .checked_add(count as u64)
            .ok_or(AdapterError::QuotaExceeded("secure random bytes"))?;
        if next > limit {
            return Err(AdapterError::QuotaExceeded("secure random bytes"));
        }
        let mut output = vec![0; count];
        self.secure.fill_secure(&mut output)?;
        self.secure_bytes = next;
        Ok(output)
    }

    pub fn insecure_u64(&mut self) -> Result<u64, AdapterError> {
        if !matches!(
            &self.policy,
            RandomPolicy::SecureAndDeterministicInsecure { .. }
        ) {
            return Err(AdapterError::Denied("insecure random"));
        }
        let result = self.insecure_state[0]
            .wrapping_add(self.insecure_state[3])
            .rotate_left(23)
            .wrapping_add(self.insecure_state[0]);
        let temporary = self.insecure_state[1] << 17;
        self.insecure_state[2] ^= self.insecure_state[0];
        self.insecure_state[3] ^= self.insecure_state[1];
        self.insecure_state[1] ^= self.insecure_state[2];
        self.insecure_state[0] ^= self.insecure_state[3];
        self.insecure_state[2] ^= temporary;
        self.insecure_state[3] = self.insecure_state[3].rotate_left(45);
        Ok(result)
    }
}

#[derive(Clone, Debug)]
pub struct FilesystemAdapter {
    mounts: Vec<Mount>,
}

impl FilesystemAdapter {
    pub fn new(mounts: Vec<Mount>) -> Result<Self, AdapterError> {
        if mounts.is_empty() {
            return Err(AdapterError::Denied("filesystem preopens"));
        }
        let mut guest_paths: Vec<&str> = Vec::with_capacity(mounts.len());
        for mount in &mounts {
            if !Self::valid_guest_preopen(&mount.guest_path) {
                return Err(AdapterError::InvalidArgument("invalid guest preopen path"));
            }
            if guest_paths
                .iter()
                .any(|existing| Self::preopens_overlap(existing, &mount.guest_path))
            {
                return Err(AdapterError::InvalidArgument(
                    "duplicate or overlapping guest preopen path",
                ));
            }
            guest_paths.push(&mount.guest_path);
            validate_mount_limits(mount.limits)?;
        }
        Ok(Self { mounts })
    }

    fn preopens_overlap(left: &str, right: &str) -> bool {
        left == right
            || left
                .strip_prefix(right)
                .is_some_and(|suffix| suffix.starts_with('/'))
            || right
                .strip_prefix(left)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }

    fn valid_guest_preopen(path: &str) -> bool {
        path.starts_with('/')
            && !path.contains('\0')
            && path != "/"
            && path
                .split('/')
                .skip(1)
                .all(|component| !component.is_empty() && component != "." && component != "..")
    }

    pub fn mounts(&self) -> &[Mount] {
        &self.mounts
    }
}

fn validate_mount_limits(limits: MountLimits) -> Result<(), AdapterError> {
    if limits.max_operations.is_none_or(|value| value == 0)
        || limits.max_read_bytes.is_none_or(|value| value == 0)
        || limits.max_write_bytes.is_none_or(|value| value == 0)
    {
        return Err(AdapterError::InvalidArgument(
            "preopens require finite nonzero operation/read/write limits",
        ));
    }
    Ok(())
}

#[derive(Clone)]
pub struct SocketsAdapter {
    runtime: BrokerRuntime,
}

impl SocketsAdapter {
    pub fn new(runtime: BrokerRuntime) -> Result<Self, AdapterError> {
        if !runtime.has_network() {
            return Err(AdapterError::Denied("network broker"));
        }
        Ok(Self { runtime })
    }

    pub fn runtime(&self) -> &BrokerRuntime {
        &self.runtime
    }

    pub fn listen_or_accept(&self) -> Result<(), AdapterError> {
        Err(AdapterError::NotSupported("inbound listen and accept"))
    }

    pub fn udp(&self) -> Result<(), AdapterError> {
        Err(AdapterError::NotSupported(
            "UDP broker operation tags are not frozen",
        ))
    }
}

pub trait IncomingHttpHandler: Send {
    fn handle(&mut self, request: RequestEnvelope) -> Result<ResponseEnvelope, AdapterError>;
}

pub struct HttpAdapter {
    broker: FetchBroker,
    incoming: Option<Box<dyn IncomingHttpHandler>>,
}

impl HttpAdapter {
    pub fn new(broker: FetchBroker) -> Self {
        Self {
            broker,
            incoming: None,
        }
    }

    pub fn with_incoming_handler(mut self, incoming: impl IncomingHttpHandler + 'static) -> Self {
        self.incoming = Some(Box::new(incoming));
        self
    }

    pub fn broker(&self) -> &FetchBroker {
        &self.broker
    }

    pub fn handle_incoming(
        &mut self,
        request: RequestEnvelope,
    ) -> Result<ResponseEnvelope, AdapterError> {
        self.incoming
            .as_mut()
            .ok_or(AdapterError::Denied("HTTP incoming handler"))?
            .handle(request)
    }

    pub fn supports_incoming_handler(&self) -> bool {
        self.incoming.is_some()
    }
}

impl fmt::Debug for HttpAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpAdapter")
            .field("broker", &self.broker)
            .field("incoming", &self.incoming.is_some())
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct CliAdapter {
    environment: BTreeMap<String, String>,
    arguments: Vec<String>,
    stdin: InputStream,
    stdout: OutputStream,
    stderr: OutputStream,
    allow_exit: bool,
    allow_run: bool,
    exit_status: Option<Result<(), ()>>,
    run_claimed: bool,
}

impl CliAdapter {
    pub fn new(policy: &CliPolicy, stdin: Vec<u8>) -> Result<Self, AdapterError> {
        if stdin.len() > policy.max_stdin_bytes() {
            return Err(AdapterError::QuotaExceeded("stdin bytes"));
        }
        Ok(Self {
            environment: policy.environment().clone(),
            arguments: policy.arguments().to_vec(),
            stdin: InputStream::new(
                stdin,
                policy.max_stdin_bytes(),
                policy.max_stdin_bytes().min(32 * 1024),
            )?,
            stdout: OutputStream::new(
                policy.max_stdout_bytes(),
                policy.max_stdout_bytes().min(32 * 1024),
            )?,
            stderr: OutputStream::new(
                policy.max_stderr_bytes(),
                policy.max_stderr_bytes().min(32 * 1024),
            )?,
            allow_exit: policy.allows_exit(),
            allow_run: policy.allows_run(),
            exit_status: None,
            run_claimed: false,
        })
    }

    pub fn environment(&self) -> &BTreeMap<String, String> {
        &self.environment
    }

    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    pub fn stdin(&mut self) -> &mut InputStream {
        &mut self.stdin
    }

    pub fn stdout(&mut self) -> &mut OutputStream {
        &mut self.stdout
    }

    pub fn stderr(&mut self) -> &mut OutputStream {
        &mut self.stderr
    }

    pub fn claim_run(&mut self) -> Result<(), AdapterError> {
        if !self.allow_run {
            return Err(AdapterError::Denied("cli run"));
        }
        if self.run_claimed {
            return Err(AdapterError::InvalidArgument("cli run already claimed"));
        }
        self.run_claimed = true;
        Ok(())
    }

    pub fn exit(&mut self, success: bool) -> Result<(), AdapterError> {
        if !self.allow_exit {
            return Err(AdapterError::Denied("cli exit"));
        }
        self.exit_status = Some(if success { Ok(()) } else { Err(()) });
        Ok(())
    }

    pub fn exit_status(&self) -> Option<Result<(), ()>> {
        self.exit_status
    }

    pub fn terminal(&self) -> Result<(), AdapterError> {
        Err(AdapterError::NotSupported("terminal and raw device access"))
    }
}

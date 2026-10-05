use std::collections::BTreeMap;
use std::fmt;

const P2_INIT_ENVELOPE_BYTES: usize = 60 * 1024;

pub const WASI_P2_VERSION: &str = "0.2.12";
pub const WASI_P2_COMMIT: &str = "281ba75fafcd50961ef55f9e52747afcc9b71ede";
pub const P2_LOCK_SHA256: &str = "f4f8227f70056750a91c8ff1712138e3a5ff65dd9586042e2dedf2edd847ce28";
pub const POLICY_ABI_SHA256: &str =
    "d4f1316d59594640688a2d8a3875fea675dc51196f36e19024d275675e273447";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PackageLock {
    pub package: &'static str,
    pub tree: &'static str,
}

pub const PACKAGE_LOCKS: &[PackageLock] = &[
    PackageLock {
        package: "wasi:cli@0.2.12",
        tree: "ac056a919cc9a0b373a11405a581109ba53c01ba",
    },
    PackageLock {
        package: "wasi:clocks@0.2.12",
        tree: "4b20613aa5b9a38d9810ddeb899adeceb781703e",
    },
    PackageLock {
        package: "wasi:filesystem@0.2.12",
        tree: "c1ce35f50247db025b4fadf0230e9b6823d2cfa6",
    },
    PackageLock {
        package: "wasi:http@0.2.12",
        tree: "a2a4a95967bddcb59f57801d1c817010f679b63c",
    },
    PackageLock {
        package: "wasi:io@0.2.12",
        tree: "7ec4eda9f981d82f6040505b8effa5852efed8a1",
    },
    PackageLock {
        package: "wasi:random@0.2.12",
        tree: "7323ba44add1465047af6d8fb66d58a632a549ad",
    },
    PackageLock {
        package: "wasi:sockets@0.2.12",
        tree: "dd92ec18ab8c73b47341df84beaa6b4f4992b167",
    },
];

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum P2Import {
    IoError,
    IoPoll,
    IoStreams,
    ClocksMonotonic,
    ClocksTimezone,
    ClocksWall,
    RandomInsecureSeed,
    RandomInsecure,
    RandomSecure,
    FilesystemPreopens,
    FilesystemTypes,
    SocketsInstanceNetwork,
    SocketsIpNameLookup,
    SocketsNetwork,
    SocketsTcpCreateSocket,
    SocketsTcp,
    SocketsUdpCreateSocket,
    SocketsUdp,
    HttpIncomingHandler,
    HttpOutgoingHandler,
    HttpTypes,
    CliEnvironment,
    CliExit,
    CliStdin,
    CliStdout,
    CliStderr,
    CliTerminalInput,
    CliTerminalOutput,
    CliTerminalStdin,
    CliTerminalStdout,
    CliTerminalStderr,
    CliRun,
}

impl P2Import {
    pub fn parse(name: &str) -> Result<Self, PolicyError> {
        let import = match name {
            "wasi:io/error@0.2.12" => Self::IoError,
            "wasi:io/poll@0.2.12" => Self::IoPoll,
            "wasi:io/streams@0.2.12" => Self::IoStreams,
            "wasi:clocks/monotonic-clock@0.2.12" => Self::ClocksMonotonic,
            "wasi:clocks/timezone@0.2.12" => Self::ClocksTimezone,
            "wasi:clocks/wall-clock@0.2.12" => Self::ClocksWall,
            "wasi:random/insecure-seed@0.2.12" => Self::RandomInsecureSeed,
            "wasi:random/insecure@0.2.12" => Self::RandomInsecure,
            "wasi:random/random@0.2.12" => Self::RandomSecure,
            "wasi:filesystem/preopens@0.2.12" => Self::FilesystemPreopens,
            "wasi:filesystem/types@0.2.12" => Self::FilesystemTypes,
            "wasi:sockets/instance-network@0.2.12" => Self::SocketsInstanceNetwork,
            "wasi:sockets/ip-name-lookup@0.2.12" => Self::SocketsIpNameLookup,
            "wasi:sockets/network@0.2.12" => Self::SocketsNetwork,
            "wasi:sockets/tcp-create-socket@0.2.12" => Self::SocketsTcpCreateSocket,
            "wasi:sockets/tcp@0.2.12" => Self::SocketsTcp,
            "wasi:sockets/udp-create-socket@0.2.12" => Self::SocketsUdpCreateSocket,
            "wasi:sockets/udp@0.2.12" => Self::SocketsUdp,
            "wasi:http/incoming-handler@0.2.12" => Self::HttpIncomingHandler,
            "wasi:http/outgoing-handler@0.2.12" => Self::HttpOutgoingHandler,
            "wasi:http/types@0.2.12" => Self::HttpTypes,
            "wasi:cli/environment@0.2.12" => Self::CliEnvironment,
            "wasi:cli/exit@0.2.12" => Self::CliExit,
            "wasi:cli/stdin@0.2.12" => Self::CliStdin,
            "wasi:cli/stdout@0.2.12" => Self::CliStdout,
            "wasi:cli/stderr@0.2.12" => Self::CliStderr,
            "wasi:cli/terminal-input@0.2.12" => Self::CliTerminalInput,
            "wasi:cli/terminal-output@0.2.12" => Self::CliTerminalOutput,
            "wasi:cli/terminal-stdin@0.2.12" => Self::CliTerminalStdin,
            "wasi:cli/terminal-stdout@0.2.12" => Self::CliTerminalStdout,
            "wasi:cli/terminal-stderr@0.2.12" => Self::CliTerminalStderr,
            "wasi:cli/run@0.2.12" => Self::CliRun,
            _ => return Err(PolicyError::UnsupportedImport(name.to_string())),
        };
        Ok(import)
    }

    pub fn canonical_name(self) -> &'static str {
        match self {
            Self::IoError => "wasi:io/error@0.2.12",
            Self::IoPoll => "wasi:io/poll@0.2.12",
            Self::IoStreams => "wasi:io/streams@0.2.12",
            Self::ClocksMonotonic => "wasi:clocks/monotonic-clock@0.2.12",
            Self::ClocksTimezone => "wasi:clocks/timezone@0.2.12",
            Self::ClocksWall => "wasi:clocks/wall-clock@0.2.12",
            Self::RandomInsecureSeed => "wasi:random/insecure-seed@0.2.12",
            Self::RandomInsecure => "wasi:random/insecure@0.2.12",
            Self::RandomSecure => "wasi:random/random@0.2.12",
            Self::FilesystemPreopens => "wasi:filesystem/preopens@0.2.12",
            Self::FilesystemTypes => "wasi:filesystem/types@0.2.12",
            Self::SocketsInstanceNetwork => "wasi:sockets/instance-network@0.2.12",
            Self::SocketsIpNameLookup => "wasi:sockets/ip-name-lookup@0.2.12",
            Self::SocketsNetwork => "wasi:sockets/network@0.2.12",
            Self::SocketsTcpCreateSocket => "wasi:sockets/tcp-create-socket@0.2.12",
            Self::SocketsTcp => "wasi:sockets/tcp@0.2.12",
            Self::SocketsUdpCreateSocket => "wasi:sockets/udp-create-socket@0.2.12",
            Self::SocketsUdp => "wasi:sockets/udp@0.2.12",
            Self::HttpIncomingHandler => "wasi:http/incoming-handler@0.2.12",
            Self::HttpOutgoingHandler => "wasi:http/outgoing-handler@0.2.12",
            Self::HttpTypes => "wasi:http/types@0.2.12",
            Self::CliEnvironment => "wasi:cli/environment@0.2.12",
            Self::CliExit => "wasi:cli/exit@0.2.12",
            Self::CliStdin => "wasi:cli/stdin@0.2.12",
            Self::CliStdout => "wasi:cli/stdout@0.2.12",
            Self::CliStderr => "wasi:cli/stderr@0.2.12",
            Self::CliTerminalInput => "wasi:cli/terminal-input@0.2.12",
            Self::CliTerminalOutput => "wasi:cli/terminal-output@0.2.12",
            Self::CliTerminalStdin => "wasi:cli/terminal-stdin@0.2.12",
            Self::CliTerminalStdout => "wasi:cli/terminal-stdout@0.2.12",
            Self::CliTerminalStderr => "wasi:cli/terminal-stderr@0.2.12",
            Self::CliRun => "wasi:cli/run@0.2.12",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InterfaceDisposition {
    Native,
    Adapted,
    Granted,
    Denied,
    NotSupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImportRoute {
    NativeIo,
    NativeHttp,
    Clock,
    Random,
    BoundedPreopens,
    NetworkBroker,
    WorkerdFetch,
    Cli,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Authorization {
    pub import: P2Import,
    pub disposition: InterfaceDisposition,
    pub route: ImportRoute,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum ClockPolicy {
    #[default]
    Denied,
    Deterministic {
        wall_epoch_ns: u64,
        monotonic_step_ns: u64,
        timezone: Option<String>,
    },
    Host {
        wall: bool,
        monotonic: bool,
        fixed_timezone: Option<String>,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum RandomPolicy {
    #[default]
    Denied,
    Secure {
        max_bytes: u64,
    },
    SecureAndDeterministicInsecure {
        max_secure_bytes: u64,
        insecure_seed: [u8; 32],
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliPolicy {
    environment: BTreeMap<String, String>,
    arguments: Vec<String>,
    max_stdin_bytes: usize,
    max_stdout_bytes: usize,
    max_stderr_bytes: usize,
    allow_exit: bool,
    allow_run: bool,
}

impl CliPolicy {
    pub fn new(
        environment: BTreeMap<String, String>,
        arguments: Vec<String>,
        max_stdin_bytes: usize,
        max_stdout_bytes: usize,
        max_stderr_bytes: usize,
        allow_exit: bool,
        allow_run: bool,
    ) -> Result<Self, PolicyError> {
        if max_stdin_bytes == 0 || max_stdout_bytes == 0 || max_stderr_bytes == 0 {
            return Err(PolicyError::InvalidPolicy("stdio limits must be nonzero"));
        }
        if environment.len() > 128 || arguments.len() > 128 {
            return Err(PolicyError::InvalidPolicy(
                "environment and argument counts are capped at 128",
            ));
        }
        for (key, value) in &environment {
            if key.is_empty()
                || key.len() > 256
                || value.len() > 16 * 1024
                || key.contains('=')
                || key.bytes().any(|byte| byte == 0)
                || value.bytes().any(|byte| byte == 0)
            {
                return Err(PolicyError::InvalidPolicy(
                    "environment entries must be bounded, NUL-free key/value pairs",
                ));
            }
        }
        if arguments
            .iter()
            .any(|argument| argument.len() > 16 * 1024 || argument.bytes().any(|byte| byte == 0))
        {
            return Err(PolicyError::InvalidPolicy(
                "arguments must be bounded and NUL-free",
            ));
        }
        let aggregate_bytes = environment
            .iter()
            .map(|(key, value)| key.len().saturating_add(value.len()).saturating_add(2))
            .chain(
                arguments
                    .iter()
                    .map(|argument| argument.len().saturating_add(1)),
            )
            .try_fold(0usize, usize::checked_add)
            .ok_or(PolicyError::InvalidPolicy(
                "CLI environment and arguments exceed the init envelope",
            ))?;
        if aggregate_bytes >= P2_INIT_ENVELOPE_BYTES {
            return Err(PolicyError::InvalidPolicy(
                "CLI environment and arguments exceed the init envelope",
            ));
        }
        Ok(Self {
            environment,
            arguments,
            max_stdin_bytes,
            max_stdout_bytes,
            max_stderr_bytes,
            allow_exit,
            allow_run,
        })
    }

    pub fn environment(&self) -> &BTreeMap<String, String> {
        &self.environment
    }

    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    pub fn max_stdin_bytes(&self) -> usize {
        self.max_stdin_bytes
    }

    pub fn max_stdout_bytes(&self) -> usize {
        self.max_stdout_bytes
    }

    pub fn max_stderr_bytes(&self) -> usize {
        self.max_stderr_bytes
    }

    pub fn allows_exit(&self) -> bool {
        self.allow_exit
    }

    pub fn allows_run(&self) -> bool {
        self.allow_run
    }
}

#[derive(Clone, Debug, Default)]
pub struct P2Policy {
    clocks: ClockPolicy,
    random: RandomPolicy,
    filesystem: bool,
    sockets: bool,
    http_client: bool,
    http_service: bool,
    cli: Option<CliPolicy>,
}

impl P2Policy {
    pub fn deny_all() -> Self {
        Self::default()
    }

    pub fn with_clocks(mut self, clocks: ClockPolicy) -> Self {
        self.clocks = clocks;
        self
    }

    pub fn with_random(mut self, random: RandomPolicy) -> Self {
        self.random = random;
        self
    }

    pub fn with_filesystem(mut self) -> Self {
        self.filesystem = true;
        self
    }

    pub fn with_sockets(mut self) -> Self {
        self.sockets = true;
        self
    }

    pub fn with_http_client(mut self) -> Self {
        self.http_client = true;
        self
    }

    pub fn with_http_service(mut self) -> Self {
        self.http_service = true;
        self
    }

    pub fn with_cli(mut self, cli: CliPolicy) -> Self {
        self.cli = Some(cli);
        self
    }

    pub(crate) fn authorize_name(&self, name: &str) -> Result<Authorization, PolicyError> {
        self.authorize(P2Import::parse(name)?)
    }

    pub(crate) fn authorize(&self, import: P2Import) -> Result<Authorization, PolicyError> {
        self.validate()?;
        use P2Import::*;
        let authorization = match import {
            IoError | IoStreams => Authorization {
                import,
                disposition: InterfaceDisposition::Native,
                route: ImportRoute::NativeIo,
            },
            IoPoll => adapted(import, ImportRoute::NativeIo),
            ClocksMonotonic if self.monotonic_clock_granted() => {
                adapted(import, ImportRoute::Clock)
            }
            ClocksWall if self.wall_clock_granted() => adapted(import, ImportRoute::Clock),
            ClocksTimezone if self.timezone_granted() => adapted(import, ImportRoute::Clock),
            ClocksMonotonic | ClocksWall | ClocksTimezone => {
                return Err(PolicyError::CapabilityDenied(import.canonical_name()));
            }
            RandomSecure if self.secure_random_granted() => adapted(import, ImportRoute::Random),
            RandomInsecure | RandomInsecureSeed if self.insecure_random_granted() => {
                Authorization {
                    import,
                    disposition: InterfaceDisposition::Native,
                    route: ImportRoute::Random,
                }
            }
            RandomSecure | RandomInsecure | RandomInsecureSeed => {
                return Err(PolicyError::CapabilityDenied(import.canonical_name()));
            }
            FilesystemPreopens | FilesystemTypes if self.filesystem => {
                adapted(import, ImportRoute::BoundedPreopens)
            }
            FilesystemPreopens | FilesystemTypes => {
                return Err(PolicyError::CapabilityDenied(import.canonical_name()));
            }
            SocketsUdpCreateSocket | SocketsUdp => {
                return Err(PolicyError::NotSupported(import.canonical_name()));
            }
            SocketsInstanceNetwork
            | SocketsIpNameLookup
            | SocketsNetwork
            | SocketsTcpCreateSocket
            | SocketsTcp
                if self.sockets =>
            {
                adapted(import, ImportRoute::NetworkBroker)
            }
            SocketsInstanceNetwork
            | SocketsIpNameLookup
            | SocketsNetwork
            | SocketsTcpCreateSocket
            | SocketsTcp => {
                return Err(PolicyError::CapabilityDenied(import.canonical_name()));
            }
            HttpTypes => Authorization {
                import,
                disposition: InterfaceDisposition::Native,
                route: ImportRoute::NativeHttp,
            },
            HttpOutgoingHandler if self.http_client => adapted(import, ImportRoute::WorkerdFetch),
            HttpIncomingHandler if self.http_service => adapted(import, ImportRoute::WorkerdFetch),
            HttpIncomingHandler => {
                return Err(PolicyError::CapabilityDenied(import.canonical_name()));
            }
            HttpOutgoingHandler => {
                return Err(PolicyError::CapabilityDenied(import.canonical_name()));
            }
            CliTerminalInput | CliTerminalOutput | CliTerminalStdin | CliTerminalStdout
            | CliTerminalStderr => {
                return Err(PolicyError::NotSupported(import.canonical_name()));
            }
            CliEnvironment if self.cli.is_some() => adapted(import, ImportRoute::Cli),
            CliStdin | CliStdout | CliStderr if self.cli.is_some() => {
                adapted(import, ImportRoute::Cli)
            }
            CliExit if self.cli.as_ref().is_some_and(CliPolicy::allows_exit) => {
                adapted(import, ImportRoute::Cli)
            }
            CliRun if self.cli.as_ref().is_some_and(CliPolicy::allows_run) => {
                adapted(import, ImportRoute::Cli)
            }
            CliEnvironment | CliExit | CliStdin | CliStdout | CliStderr | CliRun => {
                return Err(PolicyError::CapabilityDenied(import.canonical_name()));
            }
        };
        Ok(authorization)
    }

    pub fn clocks(&self) -> &ClockPolicy {
        &self.clocks
    }

    pub fn random(&self) -> &RandomPolicy {
        &self.random
    }

    pub fn cli(&self) -> Option<&CliPolicy> {
        self.cli.as_ref()
    }

    pub fn validate(&self) -> Result<(), PolicyError> {
        match &self.clocks {
            ClockPolicy::Deterministic {
                monotonic_step_ns,
                timezone,
                ..
            } => {
                if *monotonic_step_ns == 0 {
                    return Err(PolicyError::InvalidPolicy(
                        "deterministic monotonic step must be nonzero",
                    ));
                }
                if let Some(timezone) = timezone {
                    validate_timezone(timezone)?;
                }
            }
            ClockPolicy::Host { fixed_timezone, .. } => {
                if let Some(timezone) = fixed_timezone {
                    validate_timezone(timezone)?;
                }
            }
            ClockPolicy::Denied => {}
        }
        Ok(())
    }

    fn wall_clock_granted(&self) -> bool {
        matches!(
            &self.clocks,
            ClockPolicy::Deterministic { .. } | ClockPolicy::Host { wall: true, .. }
        )
    }

    fn monotonic_clock_granted(&self) -> bool {
        matches!(
            &self.clocks,
            ClockPolicy::Deterministic { .. }
                | ClockPolicy::Host {
                    monotonic: true,
                    ..
                }
        )
    }

    fn timezone_granted(&self) -> bool {
        match &self.clocks {
            ClockPolicy::Deterministic { timezone, .. } => timezone.is_some(),
            ClockPolicy::Host { fixed_timezone, .. } => fixed_timezone.is_some(),
            ClockPolicy::Denied => false,
        }
    }

    fn secure_random_granted(&self) -> bool {
        match &self.random {
            RandomPolicy::Denied => false,
            RandomPolicy::Secure { max_bytes } => *max_bytes > 0,
            RandomPolicy::SecureAndDeterministicInsecure {
                max_secure_bytes, ..
            } => *max_secure_bytes > 0,
        }
    }

    fn insecure_random_granted(&self) -> bool {
        matches!(
            &self.random,
            RandomPolicy::SecureAndDeterministicInsecure { .. }
        )
    }
}

fn adapted(import: P2Import, route: ImportRoute) -> Authorization {
    Authorization {
        import,
        disposition: InterfaceDisposition::Adapted,
        route,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PolicyError {
    UnsupportedImport(String),
    CapabilityDenied(&'static str),
    AdapterMissing(&'static str),
    NotSupported(&'static str),
    InvalidPolicy(&'static str),
}

impl PolicyError {
    pub fn stable_code(&self) -> &'static str {
        match self {
            Self::UnsupportedImport(_) => "unsupported-import",
            Self::CapabilityDenied(_) => "capability-denied",
            Self::AdapterMissing(_) => "unsupported-import",
            Self::NotSupported(_) => "not-supported",
            Self::InvalidPolicy(_) => "invalid-argument",
        }
    }
}

impl fmt::Display for PolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedImport(import) => write!(formatter, "unsupported import {import}"),
            Self::CapabilityDenied(import) => write!(formatter, "capability denied for {import}"),
            Self::AdapterMissing(adapter) => {
                write!(formatter, "required adapter is not registered: {adapter}")
            }
            Self::NotSupported(import) => write!(formatter, "{import} is not supported"),
            Self::InvalidPolicy(reason) => write!(formatter, "invalid policy: {reason}"),
        }
    }
}

impl std::error::Error for PolicyError {}

fn validate_timezone(timezone: &str) -> Result<(), PolicyError> {
    if timezone.is_empty()
        || timezone.len() > 64
        || timezone.contains("..")
        || !timezone.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'_' | b'+' | b'-' | b':')
        })
    {
        return Err(PolicyError::InvalidPolicy(
            "fixed timezone must be a bounded canonical name or UTC offset",
        ));
    }
    if let Some(offset) = timezone
        .strip_prefix("UTC+")
        .or_else(|| timezone.strip_prefix("UTC-"))
    {
        let Some((hours, minutes)) = offset.split_once(':') else {
            return Err(PolicyError::InvalidPolicy(
                "UTC offsets must use UTC+HH:MM or UTC-HH:MM",
            ));
        };
        let valid = hours.len() == 2
            && minutes.len() == 2
            && hours.parse::<u8>().is_ok_and(|value| value <= 23)
            && minutes.parse::<u8>().is_ok_and(|value| value <= 59);
        if !valid {
            return Err(PolicyError::InvalidPolicy(
                "UTC offset is outside the supported range",
            ));
        }
    }
    Ok(())
}

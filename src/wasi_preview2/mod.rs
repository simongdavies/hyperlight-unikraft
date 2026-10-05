//! WASI Preview 2 (`0.2.12`) capability adapters.
//!
//! This module translates WIT-facing imports onto existing bounded host
//! capabilities. It does not register ambient filesystem, socket, environment,
//! terminal, device, process, or shell authority.

mod adapters;
mod policy;
mod proof;
mod registry;

pub use adapters::{
    AdapterError, CliAdapter, ClockAdapter, ClockBackend, FilesystemAdapter, HttpAdapter,
    IncomingHttpHandler, InputStream, OutputStream, PollSet, RandomAdapter, SecureRandom,
    SocketsAdapter,
};
pub use policy::{
    Authorization, CliPolicy, ClockPolicy, ImportRoute, InterfaceDisposition, P2_LOCK_SHA256,
    P2Import, P2Policy, PACKAGE_LOCKS, POLICY_ABI_SHA256, PackageLock, PolicyError, RandomPolicy,
    WASI_P2_COMMIT, WASI_P2_VERSION,
};
pub use proof::{EXCLUSIONS_SHA256, GENERATION_PROOF_SHA256, KvmProofState, P2ProofIdentity};
pub use registry::{P2AdapterRegistry, P2InstantiationPlan};

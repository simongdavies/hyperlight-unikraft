use super::{
    Authorization, CliAdapter, ClockAdapter, ClockBackend, FilesystemAdapter, HttpAdapter,
    ImportRoute, P2Import, P2Policy, PolicyError, RandomAdapter, SecureRandom, SocketsAdapter,
};

pub struct P2AdapterRegistry<'a, C, R> {
    clocks: Option<&'a ClockAdapter<C>>,
    random: Option<&'a RandomAdapter<R>>,
    filesystem: Option<&'a FilesystemAdapter>,
    sockets: Option<&'a SocketsAdapter>,
    http: Option<&'a HttpAdapter>,
    cli: Option<&'a CliAdapter>,
}

impl<'a, C, R> Default for P2AdapterRegistry<'a, C, R> {
    fn default() -> Self {
        Self {
            clocks: None,
            random: None,
            filesystem: None,
            sockets: None,
            http: None,
            cli: None,
        }
    }
}

impl<'a, C: ClockBackend, R: SecureRandom> P2AdapterRegistry<'a, C, R> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_clocks(mut self, clocks: &'a ClockAdapter<C>) -> Self {
        self.clocks = Some(clocks);
        self
    }

    pub fn with_random(mut self, random: &'a RandomAdapter<R>) -> Self {
        self.random = Some(random);
        self
    }

    pub fn with_filesystem(mut self, filesystem: &'a FilesystemAdapter) -> Self {
        self.filesystem = Some(filesystem);
        self
    }

    pub fn with_sockets(mut self, sockets: &'a SocketsAdapter) -> Self {
        self.sockets = Some(sockets);
        self
    }

    pub fn with_http(mut self, http: &'a HttpAdapter) -> Self {
        self.http = Some(http);
        self
    }

    pub fn with_cli(mut self, cli: &'a CliAdapter) -> Self {
        self.cli = Some(cli);
        self
    }

    pub fn plan<'n>(
        &self,
        policy: &P2Policy,
        imports: impl IntoIterator<Item = &'n str>,
    ) -> Result<P2InstantiationPlan, PolicyError> {
        let authorizations = imports
            .into_iter()
            .map(|name| policy.authorize_name(name))
            .collect::<Result<Vec<_>, _>>()?;
        for authorization in &authorizations {
            self.require_adapter(*authorization)?;
        }
        Ok(P2InstantiationPlan { authorizations })
    }

    fn require_adapter(&self, authorization: Authorization) -> Result<(), PolicyError> {
        let present = match authorization.route {
            ImportRoute::NativeIo | ImportRoute::NativeHttp => true,
            ImportRoute::Clock => self.clocks.is_some(),
            ImportRoute::Random => self.random.is_some(),
            ImportRoute::BoundedPreopens => self.filesystem.is_some(),
            ImportRoute::NetworkBroker => self.sockets.is_some(),
            ImportRoute::WorkerdFetch => self.http.is_some(),
            ImportRoute::Cli => self.cli.is_some(),
        };
        if !present {
            return Err(PolicyError::AdapterMissing(adapter_name(
                authorization.route,
            )));
        }
        if authorization.import == P2Import::HttpIncomingHandler
            && !self
                .http
                .is_some_and(HttpAdapter::supports_incoming_handler)
        {
            return Err(PolicyError::AdapterMissing("HTTP incoming handler"));
        }
        Ok(())
    }
}

pub struct P2InstantiationPlan {
    authorizations: Vec<Authorization>,
}

impl P2InstantiationPlan {
    pub fn authorizations(&self) -> &[Authorization] {
        &self.authorizations
    }
}

fn adapter_name(route: ImportRoute) -> &'static str {
    match route {
        ImportRoute::NativeIo => "native I/O",
        ImportRoute::NativeHttp => "native HTTP types",
        ImportRoute::Clock => "clock",
        ImportRoute::Random => "random",
        ImportRoute::BoundedPreopens => "filesystem preopen",
        ImportRoute::NetworkBroker => "network broker",
        ImportRoute::WorkerdFetch => "Workerd HTTP",
        ImportRoute::Cli => "CLI",
    }
}

use super::*;

// An unvalidated wire response. The importer and transport credits stay owned
// across buffering and transfer into the eventual import without reacquisition.
// Possession does not establish a block hash, height, validity or chain state.
pub struct Download<O = Otp26> {
    owner: Arc<Context<O>>,
    reservation: blocking::Reservation,
    live: Box<Live>,
    requested: Hash256,
    direction: Direction,
}
impl<O> Download<O> {
    pub fn requested(&self) -> Hash256 {
        self.requested
    }
    pub fn direction(&self) -> Direction {
        self.direction
    }
    pub fn peer(&self) -> ae_types::PeerPublicKey {
        self.live.client.peer()
    }
    pub fn generation(&self) -> &net::Generation {
        self.live.client.generation()
    }
}
impl<O> std::fmt::Debug for Download<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Download")
            .field("requested", &self.requested)
            .field("direction", &self.direction)
            .field("peer", &self.peer())
            .finish_non_exhaustive()
    }
}
fn empty(error: Error) -> Failure {
    Failure {
        progress: Some(Report::default()),
        error,
    }
}
fn unknown_timeout() -> Failure {
    Failure {
        progress: None,
        error: Error::Timeout,
    }
}
impl<O: MapOrder + Send + Sync + 'static> Importer<O> {
    async fn admitted(&self, owner: &network::Handle, client: &net::Client) -> Result<(), Error> {
        self.check_open()?;
        if !owner.uses_database(&self.context.db) {
            return Err(Error::Configuration);
        }
        if client.is_closed() || owner.connection(client.clone())?.wait().await?.is_none() {
            return Err(Error::Stale);
        }
        Ok(())
    }
    pub async fn download(
        &self,
        owner: &network::Handle,
        client: net::Client,
        requested: Hash256,
        direction: Direction,
    ) -> Result<Download<O>, Failure> {
        let operation = async {
            self.check_open().map_err(empty)?;
            if !owner.uses_database(&self.context.db) {
                return Err(empty(Error::Configuration));
            }
            let limits = self.context.config.messages;
            let transport = client.message_limits();
            if transport.body_bytes > limits.body_bytes
                || transport.work_bytes > limits.work_bytes
                || transport.items > limits.items
                || transport.depth > limits.depth
            {
                return Err(empty(Error::Configuration));
            }
            let reservation = self.reserve().map_err(empty)?;
            let mut shutdown = self.context.shutdown.subscribe();
            let receive = async {
                self.admitted(owner, &client).await.map_err(empty)?;
                let request = Message::new(
                    Kind::GetGeneration,
                    1,
                    BTreeMap::from([
                        ("hash".into(), Value::Binary(requested.0.to_vec())),
                        (
                            "forward".into(),
                            Value::Boolean(direction == Direction::Forward),
                        ),
                    ]),
                    limits,
                )
                .map_err(|e| empty(e.into()))?;
                let reply = client
                    .request(&request, None)
                    .map_err(|e| empty(e.into()))?
                    .await
                    .map_err(|e| empty(e.into()))?;
                let net::Reply::Response(received) = reply else {
                    return Err(empty(Error::Stale));
                };
                if received.generation != *client.generation() {
                    return Err(empty(Error::Stale));
                }
                self.admitted(owner, &client).await.map_err(empty)?;
                Ok(Box::new(Live { received, client }))
            };
            let live = tokio::select! {
                biased;
                _=shutdown.wait_for(|stopped|*stopped)=>return Err(empty(Error::Closed)),
                result=receive=>result?,
            };
            Ok(Download {
                owner: self.context.clone(),
                reservation,
                live,
                requested,
                direction,
            })
        };
        // This operation cannot submit a write, so its timeout has an exact
        // empty committed prefix. Dropping a pending wire request closes it.
        tokio::time::timeout(self.context.config.timeout, operation)
            .await
            .map_err(|_| empty(Error::Timeout))?
    }
    pub async fn import_download(
        &self,
        owner: &network::Handle,
        download: Download<O>,
        now_ms: u64,
    ) -> Result<Report, Failure> {
        let operation = async {
            self.check_open().map_err(empty)?;
            if !Arc::ptr_eq(&self.context, &download.owner) {
                return Err(empty(Error::Configuration));
            }
            let mut shutdown = self.context.shutdown.subscribe();
            tokio::select! {
                biased;
                _=shutdown.wait_for(|stopped|*stopped)=>return Err(empty(Error::Closed)),
                result=self.admitted(owner,&download.live.client)=>result.map_err(empty)?,
            }
            let Download {
                owner: _,
                reservation,
                live,
                requested,
                direction,
            } = download;
            self.submit(reservation, Input::Live(live), requested, direction, now_ms)
                .map_err(empty)?
                .wait()
                .await
        };
        // Once submitted, cancellation/timeout can race a durable commit.
        tokio::time::timeout(self.context.config.timeout, operation)
            .await
            .map_err(|_| unknown_timeout())?
    }
    pub async fn fetch(
        &self,
        owner: &network::Handle,
        client: net::Client,
        requested: Hash256,
        direction: Direction,
        now_ms: u64,
    ) -> Result<Report, Failure> {
        let operation = async {
            let download = self.download(owner, client, requested, direction).await?;
            self.import_download(owner, download, now_ms).await
        };
        // Preserve the existing whole-fetch deadline, including import time.
        tokio::time::timeout(self.context.config.timeout, operation)
            .await
            .map_err(|_| unknown_timeout())?
    }
}

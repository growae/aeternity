use super::*;

pub(super) enum Request {
    Node {
        hash: Hash256,
        reply: SyncSender<Result<Option<Vec<u8>>, TrieError>>,
    },
    Get {
        key: Vec<u8>,
        reply: SyncSender<Result<Option<Vec<u8>>, repo::Error>>,
    },
    Scan {
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: usize,
        reply: SyncSender<Result<repo::Entries, repo::Error>>,
    },
}
impl Request {
    pub(super) fn run(self, snapshot: &Snapshot<'_>, config: Config) {
        // Each private one-slot reply is used exactly once. An abandoned
        // caller never blocks this worker or transfers its result elsewhere.
        match self {
            Self::Node { hash, reply } => {
                let _ = reply.send(node(snapshot, hash, config.maximum_node_bytes));
            }
            Self::Get { key, reply } => {
                let result = snapshot
                    .snapshot
                    .get_pinned(key)
                    .map_err(|e| repo::Error::Backend(e.to_string()))
                    .and_then(|bytes| match bytes {
                        Some(bytes) if bytes.len() > config.maximum_read_bytes => {
                            Err(repo::Error::ResourceLimit)
                        }
                        bytes => Ok(bytes.map(|b| b.to_vec())),
                    });
                let _ = reply.send(result);
            }
            Self::Scan {
                start,
                end,
                limit,
                reply,
            } => {
                let _ = reply.send(scan(snapshot, &start, end.as_deref(), limit, config));
            }
        }
    }
}
fn scan(
    snapshot: &Snapshot<'_>,
    start: &[u8],
    end: Option<&[u8]>,
    limit: usize,
    config: Config,
) -> Result<repo::Entries, repo::Error> {
    let mut iterator = snapshot.snapshot.raw_iterator();
    iterator.seek(start);
    let mut out = vec![];
    let mut bytes = 0usize;
    while let Some(key) = iterator.key() {
        if end.map_or_else(|| !key.starts_with(start), |end| key >= end) {
            break;
        }
        let value = iterator
            .value()
            .ok_or(repo::Error::Corrupt("snapshot iterator value"))?;
        bytes = bytes
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or(repo::Error::ResourceLimit)?;
        if out.len() >= limit.min(config.maximum_scan_entries) || bytes > config.maximum_read_bytes
        {
            return Err(repo::Error::ResourceLimit);
        }
        out.push((key.to_vec(), value.to_vec()));
        iterator.next();
    }
    iterator
        .status()
        .map_err(|e| repo::Error::Backend(e.to_string()))?;
    Ok(out)
}
impl Reader {
    fn request<T>(
        &self,
        make: impl FnOnce(SyncSender<Result<T, repo::Error>>) -> Request,
    ) -> Result<T, repo::Error> {
        let (reply, receive) = mpsc::sync_channel(1);
        self.0
            .sender
            .as_ref()
            .expect("live reader has a sender")
            .try_send(make(reply))
            .map_err(|e| match e {
                TrySendError::Full(_) => repo::Error::ResourceLimit,
                TrySendError::Disconnected(_) => {
                    repo::Error::Backend("pinned snapshot worker ended".into())
                }
            })?;
        receive
            .recv()
            .map_err(|_| repo::Error::Backend("pinned snapshot worker ended".into()))?
    }
    fn scan(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<repo::Entries, repo::Error> {
        if start.len().saturating_add(end.map_or(0, <[u8]>::len)) > self.0.config.maximum_read_bytes
        {
            return Err(repo::Error::ResourceLimit);
        }
        self.request(|reply| Request::Scan {
            start: start.to_vec(),
            end: end.map(<[u8]>::to_vec),
            limit,
            reply,
        })
    }
}
impl repo::Read for Reader {
    fn source_id(&self) -> u64 {
        self.0
            .metadata
            .as_ref()
            .expect("initialized reader")
            .source_id
    }
    fn resilience(&self) -> resilience::Snapshot {
        self.0
            .metadata
            .as_ref()
            .expect("initialized reader")
            .resilience
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, repo::Error> {
        if key.len() > self.0.config.maximum_read_bytes {
            return Err(repo::Error::ResourceLimit);
        }
        self.request(|reply| Request::Get {
            key: key.to_vec(),
            reply,
        })
    }
    fn prefix(&self, prefix: &[u8], limit: usize) -> Result<repo::Entries, repo::Error> {
        self.scan(prefix, None, limit)
    }
    fn range(&self, start: &[u8], end: &[u8], limit: usize) -> Result<repo::Entries, repo::Error> {
        self.scan(start, Some(end), limit)
    }
    fn visit_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        visit: &mut repo::Visitor<'_>,
    ) -> Result<(), repo::Error> {
        for (key, value) in self.range(start, end, limit)? {
            visit(&key, &value)?;
        }
        Ok(())
    }
}

use super::*;
use crate::registry::startup::{Configuration, Origin, Reason, Settings};
use ae_p2p::{
    address,
    registry::{Info, Rejection},
};

fn uri(id: u64, host: &[u8], port: u16) -> Vec<u8> {
    address::encode(key(id), host, &port.into(), address::Limits::default()).unwrap()
}
fn configuration() -> Configuration {
    Configuration {
        include_default_peers: false,
        ..Default::default()
    }
}
fn seed(database: &Arc<Database>, peers: Vec<discovery::Peer>) {
    let mut writer = Writer::start(database.clone(), Default::default()).unwrap();
    writer
        .commit(
            writer.version().unwrap(),
            &peers
                .iter()
                .map(|p| peers::Change::Put(record(p)))
                .collect::<Vec<_>>(),
        )
        .unwrap();
}
fn resolve_effect(service: &mut Service) -> Token {
    drain(&mut service.core)
        .into_iter()
        .find_map(|e| {
            if let Effect::Resolve { token, .. } = e {
                Some(token)
            } else {
                None
            }
        })
        .unwrap()
}
use ae_p2p::registry::Token;
#[test]
fn startup_defaults_are_bound_to_the_database_network_and_can_be_disabled_or_overridden() {
    for (network, expected) in [
        (b"ae_mainnet".as_slice(), 9),
        (b"ae_uat".as_slice(), 3),
        (b"private".as_slice(), 0),
    ] {
        let temp = Temp::new();
        let (profile, genesis_network) = match network {
            b"ae_mainnet" => (NetworkProfile::mainnet(), Network::Mainnet),
            b"ae_uat" => (NetworkProfile::uat(), Network::Uat),
            _ => (
                NetworkProfile::custom(
                    network.to_vec(),
                    NetworkProfile::mainnet().activations().to_vec(),
                )
                .unwrap(),
                Network::Mainnet,
            ),
        };
        let identity = Identity::new(
            &profile,
            genesis::header(genesis_network).unwrap().hash().unwrap(),
            Hash256::ZERO,
        );
        let database = Arc::new(Database::open(&temp.0, identity).unwrap());
        let mut owner = service(database.clone(), config());
        let status = owner
            .configure_startup(0, Default::default(), Default::default())
            .unwrap();
        assert!(status.ready);
        assert_eq!(status.queued, expected);
        assert_eq!(status.protected_trusted, expected);
        assert_eq!(status.input_entries, expected);
        assert!(owner.next_effect().unwrap().is_none());
        assert_eq!(owner.version().revision(), 0);
        drop(owner);
        let mut owner = service(database.clone(), config());
        let c = Configuration {
            defaults: Some(vec![uri(1, b"127.0.0.1", 3015)]),
            ..Default::default()
        };
        assert_eq!(
            owner
                .configure_startup(0, c, Default::default())
                .unwrap()
                .queued,
            1
        );
        drop(owner);
        let mut owner = service(database, config());
        let c = Configuration {
            defaults: Some(vec![b"malformed".to_vec()]),
            ..configuration()
        };
        let status = owner.configure_startup(0, c, Default::default()).unwrap();
        assert_eq!(status.queued, 0);
        assert_eq!(status.diagnostics, 0);
    }
}
#[test]
fn startup_blocks_install_atomically_and_trusted_identity_protection_survives_pending_dns() {
    let temp = Temp::new();
    let database = db(&temp);
    seed(&database, vec![peer(1, false), peer(2, false)]);
    let before = durable(&database);
    let block = address::encode(
        key(2),
        b"[::1]",
        &address::Port::from_decimal("18446744073709551616").unwrap(),
        Default::default(),
    )
    .unwrap();
    let mut owner = service(database.clone(), config());
    let status = owner
        .configure_startup(
            1,
            Configuration {
                peers: vec![uri(1, b"configured", 3015), uri(2, b"other", 3016)],
                blocked_peers: vec![block],
                ..configuration()
            },
            Default::default(),
        )
        .unwrap();
    assert!(status.ready);
    assert_eq!(status.queued, 1);
    assert_eq!(owner.version().revision(), 2);
    assert_eq!(
        durable(&database),
        BTreeMap::from([(key(1), before[&key(1)].clone())])
    );
    let entry = owner.policy().blocked().next().unwrap();
    assert_eq!(entry.host, b"::1");
    assert_eq!(
        entry.port.as_ref().unwrap().as_decimal(),
        "18446744073709551616"
    );
    let revision = owner.policy().revision();
    assert!(matches!(
        owner.apply(2, Command::Add(peer(1, false))),
        Err(Error::TrustedPending)
    ));
    assert_eq!(owner.policy().revision(), revision);
    assert!(matches!(
        owner.apply(
            2,
            Command::Resolve {
                peer: Info::from(&peer(1, false)),
                source: None,
                trusted: false
            }
        ),
        Err(Error::TrustedPending)
    ));
    let Outcome::Reserved(token) = owner
        .apply(
            2,
            Command::ReserveInbound {
                address: Ipv4Addr::new(1, 2, 3, 4),
            },
        )
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        owner
            .apply(
                2,
                Command::Accept {
                    token,
                    peer: Info {
                        id: key(1),
                        host: b"configured".to_vec(),
                        port: 3015
                    }
                }
            )
            .unwrap(),
        Outcome::Rejected(Rejection::TrustedPending)
    );
    assert_eq!(owner.policy().counts().pending_inbound, 0);
    let effects = drain(&mut owner.core);
    assert!(effects.contains(&Effect::Blocked(key(2))));
    assert!(effects.contains(&Effect::Close(token)));
    let status = owner.advance_startup(3, 1).unwrap();
    assert_eq!(status.queued, 0);
    assert_eq!(status.protected_trusted, 1);
    let token = resolve_effect(&mut owner);
    let ip = Ipv4Addr::new(9, 8, 7, 6);
    owner
        .apply(
            4,
            Command::Resolved {
                token,
                address: Some(ip),
            },
        )
        .unwrap();
    let p = owner.policy().pool().find(key(1)).unwrap();
    assert!(p.trusted);
    assert_eq!(p.host, b"configured");
    assert_eq!(p.address, ip);
    assert_eq!(p.source, ip);
    assert_eq!(owner.startup_status().unwrap().protected_trusted, 0);
    assert_eq!(owner.startup_status().unwrap().trusted_admitted, 1);
    assert_eq!(
        durable(&database),
        BTreeMap::from([(key(1), before[&key(1)].clone())])
    );
    let mut gossip = p.clone();
    gossip.trusted = false;
    gossip.host = b"gossip".to_vec();
    owner.apply(5, Command::Add(gossip)).unwrap();
    assert!(owner.policy().pool().find(key(1)).unwrap().trusted);
}
#[test]
fn startup_restoration_streams_past_host_backpressure_and_preserves_sources_and_quarantined_rows() {
    let temp = Temp::new();
    let database = db(&temp);
    let mut rows: Vec<_> = [1, 2, 3, 4, 5, 7, 128]
        .into_iter()
        .map(|id| record(&peer(id, false)))
        .collect();
    for row in &mut rows {
        if row.id == key(1) || row.id == key(3) {
            row.host = b"shared".to_vec();
        }
        if row.id == key(4) {
            row.source = "::1".parse().unwrap();
        }
        if row.id == key(5) {
            row.port = 0;
        }
    }
    {
        let mut w = Writer::start(database.clone(), Default::default()).unwrap();
        w.commit(
            w.version().unwrap(),
            &rows
                .iter()
                .cloned()
                .map(peers::Change::Put)
                .collect::<Vec<_>>(),
        )
        .unwrap();
    }
    let before = durable(&database);
    let mut cfg = config();
    cfg.policy.maximum_hosts = 1;
    cfg.policy.maximum_peers_per_host = 2;
    cfg.policy.maximum_outbound = 0;
    let mut owner = service(database.clone(), cfg);
    let s = owner
        .configure_startup(
            1,
            configuration(),
            Settings {
                maximum_step: 2,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(s.ready);
    assert_eq!(s.queued, 4);
    assert_eq!(s.diagnostics, 3);
    let s = owner.advance_startup(2, 2).unwrap();
    assert!(s.backpressured);
    assert_eq!(s.submitted, 1);
    assert_eq!(owner.policy().counts().hosts, 1);
    let s = owner.advance_startup(2, 1).unwrap();
    assert_eq!(s.submitted, 2);
    assert_eq!(owner.policy().counts().resolving_peers, 2);
    let token = resolve_effect(&mut owner);
    let ip = Ipv4Addr::new(11, 12, 13, 14);
    owner
        .apply(
            3,
            Command::Resolved {
                token,
                address: Some(ip),
            },
        )
        .unwrap();
    for id in [1, 3] {
        let p = owner.policy().pool().find(key(id)).unwrap();
        assert_eq!(p.address, ip);
        assert_eq!(std::net::IpAddr::from(p.source), before[&key(id)].source);
    }
    let mut updated = peer(7, false);
    updated.host = b"new-advertisement".to_vec();
    updated.address = Ipv4Addr::new(8, 8, 4, 4);
    owner.apply(4, Command::Add(updated.clone())).unwrap();
    let s = owner.advance_startup(5, 2).unwrap();
    assert_eq!(s.queued, 0);
    assert_eq!(s.submitted, 3);
    assert!(owner.startup_diagnostics(0, 10).unwrap().contains(
        &crate::registry::startup::Diagnostic {
            origin: Origin::Saved(key(7)),
            reason: Reason::Stale
        }
    ));
    let token = resolve_effect(&mut owner);
    owner
        .apply(
            6,
            Command::Resolved {
                token,
                address: Some(Ipv4Addr::new(5, 6, 7, 8)),
            },
        )
        .unwrap();
    let after = durable(&database);
    for id in [4, 5, 128] {
        assert_eq!(after[&key(id)], before[&key(id)]);
    }
    assert_eq!(after[&key(7)], record(&updated));
    assert_eq!(owner.policy().counts().hosts, 0);
}
#[test]
fn startup_delete_and_block_retire_queued_and_shared_resolver_intents_without_resurrection() {
    let temp = Temp::new();
    let database = db(&temp);
    let mut one = peer(1, false);
    one.host = b"shared".to_vec();
    let mut two = peer(2, false);
    two.host = one.host.clone();
    seed(&database, vec![one.clone(), two.clone(), peer(3, false)]);
    let mut cfg = config();
    cfg.policy.maximum_outbound = 0;
    let mut owner = service(database.clone(), cfg);
    owner
        .configure_startup(1, configuration(), Default::default())
        .unwrap();
    owner.advance_startup(2, 2).unwrap();
    let token = resolve_effect(&mut owner);
    owner.apply(3, Command::Delete(key(1))).unwrap();
    assert!(!owner.policy().resolving_peer(b"shared", key(1)));
    assert!(owner.policy().resolving_peer(b"shared", key(2)));
    assert!(drain(&mut owner.core).is_empty());
    owner.apply(4, Command::Block(Info::from(&two))).unwrap();
    let effects = drain(&mut owner.core);
    assert!(effects.contains(&Effect::CancelResolve(token)));
    assert_eq!(owner.policy().counts().hosts, 0);
    owner.apply(5, Command::Delete(key(3))).unwrap();
    assert_eq!(owner.startup_status().unwrap().queued, 0);
    assert_eq!(
        owner
            .apply(
                6,
                Command::Resolved {
                    token,
                    address: Some(Ipv4Addr::LOCALHOST)
                }
            )
            .unwrap(),
        Outcome::Stale
    );
    assert!(durable(&database).is_empty());
    assert_eq!(owner.advance_startup(7, 1).unwrap().submitted, 2);
}
#[test]
fn startup_failures_reconcile_one_complete_initial_block_batch_before_opening_admission() {
    for fault in [Fault::Before, Fault::Absent, Fault::After] {
        let temp = Temp::new();
        let database = db(&temp);
        seed(
            &database,
            vec![peer(1, false), peer(2, false), peer(3, false)],
        );
        let before = durable(&database);
        let mut core = faulty(database.clone());
        core.store.fault = fault;
        let c = Configuration {
            peers: vec![uri(3, b"configured", 3015)],
            blocked_peers: vec![uri(1, b"127.0.0.1", 1), uri(2, b"127.0.0.2", 2)],
            ..configuration()
        };
        assert!(matches!(
            core.configure_startup(1, b"ae_mainnet", c, Default::default()),
            Err(Error::Storage(_))
        ));
        assert_eq!(core.registry.revision(), 0);
        assert!(!core.startup.as_ref().unwrap().status().ready);
        assert!(core.next_effect().is_err());
        assert!(core.apply(1, Command::Add(peer(9, false))).is_err());
        if matches!(fault, Fault::After) {
            assert_eq!(
                durable(&database),
                BTreeMap::from([(key(3), before[&key(3)].clone())])
            );
            core.store.tamper = true;
            assert!(matches!(core.recover(), Err(Error::Inconsistent)));
            core.store.tamper = false;
        } else {
            assert_eq!(durable(&database), before);
        }
        let recovery = core.recover().unwrap();
        assert_eq!(
            recovery,
            match fault {
                Fault::Before => Recovery::Unchanged,
                Fault::Absent => Recovery::NotCommitted,
                Fault::After => Recovery::Committed(Outcome::Done),
                Fault::None => unreachable!(),
            }
        );
        core.advance_startup(2, 1).unwrap();
        assert_eq!(core.version.revision(), 2);
        assert!(core.startup.as_ref().unwrap().status().ready);
        assert_eq!(core.registry.blocked().count(), 2);
        let effects = drain(&mut core);
        assert_eq!(
            effects
                .iter()
                .filter(|e| matches!(e, Effect::Blocked(_)))
                .count(),
            2
        );
        assert_eq!(
            effects
                .iter()
                .filter(|e| matches!(e, Effect::Resolve { .. }))
                .count(),
            1
        );
        assert_eq!(core.recover().unwrap(), Recovery::Unchanged);
        assert!(core.next_effect().unwrap().is_none());
    }
}
#[test]
fn startup_configuration_validation_budgets_duplicates_and_retry_leave_no_partial_installation() {
    let temp = Temp::new();
    let database = db(&temp);
    seed(&database, vec![peer(1, false), peer(2, false)]);
    let before = durable(&database);
    let mut owner = service(database.clone(), config());
    assert!(matches!(
        owner.configure_startup(
            1,
            configuration(),
            Settings {
                entries: 1,
                ..Default::default()
            }
        ),
        Err(Error::Startup(
            crate::registry::startup::Error::ResourceLimit
        ))
    ));
    assert!(owner.startup_status().is_none());
    assert_eq!(durable(&database), before);
    let mut invalid = configuration();
    invalid.addresses.bytes = 0;
    assert!(matches!(
        owner.configure_startup(1, invalid, Default::default()),
        Err(Error::Startup(
            crate::registry::startup::Error::Configuration
        ))
    ));
    assert!(owner.startup_status().is_none());
    let c = Configuration {
        peers: vec![
            uri(3, b"first", 3),
            uri(3, b"first", 3),
            uri(3, b"second", 3),
            b"not-an-address".to_vec(),
        ],
        defaults: Some(vec![uri(3, b"default", 3)]),
        include_default_peers: true,
        ..configuration()
    };
    let status = owner.configure_startup(1, c, Default::default()).unwrap();
    assert_eq!(status.protected_trusted, 1);
    assert_eq!(status.queued, 3);
    assert_eq!(status.diagnostics, 4);
    assert_eq!(durable(&database), before);
    assert!(matches!(
        owner.configure_startup(2, configuration(), Default::default()),
        Err(Error::Startup(crate::registry::startup::Error::NotFresh))
    ));
    assert!(matches!(
        owner.advance_startup(2, 65),
        Err(Error::Startup(
            crate::registry::startup::Error::ResourceLimit
        ))
    ));
    assert_eq!(owner.startup_status().unwrap().submitted, 0);
    assert!(
        owner
            .startup_diagnostics(usize::MAX, 4096)
            .unwrap()
            .is_empty()
    );
    assert!(owner.startup_diagnostics(0, 4097).is_err());
    owner.advance_startup(2, 1).unwrap();
    let e = drain(&mut owner.core);
    assert!(
        e.iter()
            .any(|e| matches!(e,Effect::Resolve{host,..}if host==b"first"))
    );
    drop(owner);
    let mut limited = config();
    limited.storage.changes = 1;
    let mut owner = service(database.clone(), limited);
    let c = Configuration {
        blocked_peers: vec![uri(1, b"block", 1), uri(2, b"block", 2)],
        ..configuration()
    };
    assert!(matches!(
        owner.configure_startup(1, c.clone(), Default::default()),
        Err(Error::Storage(_))
    ));
    assert!(!owner.startup_status().unwrap().ready);
    assert_eq!(durable(&database), before);
    assert_eq!(owner.policy().revision(), 0);
    drop(owner);
    let mut owner = service(database.clone(), config());
    owner.configure_startup(1, c, Default::default()).unwrap();
    assert!(durable(&database).is_empty());
    assert_eq!(owner.version().revision(), 2);
}
#[test]
fn startup_worker_retains_cancelled_observations_and_progress_through_joined_shutdown() {
    let temp = Temp::new();
    let database = db(&temp);
    seed(
        &database,
        vec![peer(1, false), peer(2, false), peer(3, false)],
    );
    let mut cfg = config();
    cfg.policy.maximum_hosts = 1;
    cfg.policy.maximum_outbound = 0;
    let worker =
        crate::registry::worker::Worker::start(service(database.clone(), cfg), Default::default())
            .unwrap();
    let client = worker.client();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let c = Configuration {
        blocked_peers: vec![uri(3, b"block", 3)],
        ..configuration()
    };
    drop(client.configure_startup(1, c, Default::default()).unwrap());
    let state = runtime
        .block_on(client.startup_status().unwrap())
        .unwrap()
        .unwrap();
    assert!(state.ready);
    assert_eq!(state.queued, 2);
    assert!(!durable(&database).contains_key(&key(3)));
    drop(client.advance_startup(2, 1).unwrap());
    let state = runtime
        .block_on(client.startup_status().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(state.queued, 1);
    assert_eq!(state.submitted, 1);
    let first = runtime
        .block_on(client.next_effect().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(first.effect, Effect::Blocked(key(3)));
    runtime
        .block_on(client.acknowledge(first.id).unwrap())
        .unwrap();
    let next = runtime
        .block_on(client.next_effect().unwrap())
        .unwrap()
        .unwrap();
    assert!(matches!(next.effect, Effect::Resolve { .. }));
    assert!(matches!(
        client.startup_diagnostics(0, 1025),
        Err(crate::registry::worker::Error::RequestTooLarge)
    ));
    let owner = worker.shutdown().unwrap();
    assert_eq!(owner.startup_status().unwrap().queued, 1);
    assert_eq!(owner.next_effect().unwrap().unwrap(), &next);
    assert_eq!(client.outstanding(), (0, 0));
}

#[tokio::test]
async fn owned_system_resolution_honors_delivery_backpressure_and_rejects_retired_callbacks() {
    use crate::{registry::worker, resolver};
    async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(std::time::Duration::from_secs(5), future)
            .await
            .expect("bounded resolver/registry integration")
    }
    let temp = Temp::new();
    let database = db(&temp);
    let mut a = peer(2, false);
    a.host = b"localhost".to_vec();
    let mut b = peer(3, false);
    b.host = b"127.0.0.1".to_vec();
    seed(&database, vec![a.clone(), b]);
    let mut cfg = config();
    cfg.policy.maximum_outbound = 0;
    let owner = worker::Worker::start(service(database.clone(), cfg), Default::default()).unwrap();
    let client = owner.client();
    within(
        client
            .configure_startup(
                0,
                Configuration {
                    peers: vec![uri(1, b"localhost", 3015)],
                    ..configuration()
                },
                Default::default(),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        within(client.advance_startup(1, 3).unwrap())
            .await
            .unwrap()
            .queued,
        0
    );
    let first = within(client.next_effect().unwrap())
        .await
        .unwrap()
        .unwrap();
    let Effect::Resolve { token, host } = first.effect else {
        panic!()
    };
    assert_eq!(host, b"localhost");
    let resolver = resolver::Resolver::start(resolver::Settings {
        threads: 1,
        jobs: 1,
        ..Default::default()
    })
    .unwrap();
    let first_result = resolver.submit(token, host).unwrap();
    assert!(within(client.acknowledge(first.id).unwrap()).await.unwrap());
    let second = within(client.next_effect().unwrap())
        .await
        .unwrap()
        .unwrap();
    let Effect::Resolve {
        token: second_token,
        host: second_host,
    } = second.effect.clone()
    else {
        panic!()
    };
    assert!(matches!(
        resolver.submit(second_token, second_host.clone()),
        Err(resolver::Error::Busy)
    ));
    assert_eq!(
        within(client.next_effect().unwrap()).await.unwrap(),
        Some(second.clone())
    );
    let completion = within(first_result).await;
    assert_eq!(completion.result, Ok(Ipv4Addr::LOCALHOST));
    within(
        client
            .apply(
                2,
                Command::Resolved {
                    token: completion.token,
                    address: completion.result.ok(),
                },
            )
            .unwrap(),
    )
    .await
    .unwrap();
    let row = durable(&database).remove(&key(2)).unwrap();
    assert_eq!(row.address, std::net::IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_eq!(row.source, std::net::IpAddr::V4(a.source));
    assert_eq!(row.host, a.host);
    assert!(!row.trusted);
    let delayed = resolver.submit(second_token, second_host).unwrap();
    assert!(
        within(client.acknowledge(second.id).unwrap())
            .await
            .unwrap()
    );
    within(client.apply(3, Command::Delete(key(3))).unwrap())
        .await
        .unwrap();
    let cancel = within(client.next_effect().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancel.effect, Effect::CancelResolve(second_token));
    assert!(
        within(client.acknowledge(cancel.id).unwrap())
            .await
            .unwrap()
    );
    // Exercise a completion that wins the OS race after the registry retired it.
    let completion = within(delayed).await;
    assert_eq!(completion.token, second_token);
    assert_eq!(
        within(
            client
                .apply(
                    4,
                    Command::Resolved {
                        token: completion.token,
                        address: completion.result.ok()
                    }
                )
                .unwrap()
        )
        .await
        .unwrap(),
        Outcome::Stale
    );
    resolver.shutdown().await;
    let service = owner.shutdown().unwrap();
    assert!(service.policy().pool().find(key(1)).unwrap().trusted);
    assert!(service.policy().pool().find(key(3)).is_none());
    assert_eq!(service.startup_status().unwrap().protected_trusted, 0);
    assert_eq!(durable(&database).len(), 1);
    assert_eq!(database.view().unwrap().status(), Default::default());
}

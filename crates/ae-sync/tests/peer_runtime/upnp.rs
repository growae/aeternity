use super::*;
use ae_sync::{
    runtime::{connection as owner, workers},
    upnp,
};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU16, AtomicU32, Ordering},
};
use tokio::{net::UdpSocket, sync::watch};

async fn mapped(client: &upnp::Client, internal: u16, external: u16) {
    within(async {
        loop {
            let info = client.info();
            assert!(info.running, "{info:?}");
            if info
                .mapping
                .as_ref()
                .is_some_and(|m| m.internal == internal && m.external == external)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
}

async fn advertised(remote: &net::Client, node: &Node, version: u16, port: u16) {
    let reply = response(remote, &ping(node, version, false)).await;
    assert_eq!(
        body(&reply).fields()["port"],
        Value::Integer(u64::from(port).into())
    );
}

#[tokio::test]
async fn upnp_advertises_live_assignments_and_fallback_over_existing_encrypted_connections() {
    let gateway = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let std::net::SocketAddr::V4(address) = gateway.local_addr().unwrap() else {
        panic!()
    };
    let std::net::SocketAddr::V4(ssdp) = silent.local_addr().unwrap() else {
        panic!()
    };
    let records = Arc::new(Mutex::new(Vec::<(u16, u16, u32)>::new()));
    let requests = records.clone();
    let assigned = Arc::new(AtomicU16::new(5015));
    let lifetime = Arc::new(AtomicU32::new(upnp::MAPPING_LIFETIME));
    let enabled = Arc::new(AtomicBool::new(false));
    let server_assigned = assigned.clone();
    let server_lifetime = lifetime.clone();
    let server_enabled = enabled.clone();
    let (stop, mut stopped) = watch::channel(false);
    let server = tokio::spawn(async move {
        let mut bytes = [0; 64];
        loop {
            let (n, from) = tokio::select! {
                _=stopped.changed()=>break,
                packet=gateway.recv_from(&mut bytes)=>packet.unwrap(),
            };
            let reply = if n == 2 {
                assert_eq!(&bytes[..n], &[0, 0]);
                vec![0, 128, 0, 0, 0, 0, 0, 1, 203, 0, 113, 1]
            } else {
                assert_eq!(n, 12);
                assert_eq!(&bytes[..4], &[0, 2, 0, 0]);
                let internal = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
                let external = u16::from_be_bytes(bytes[6..8].try_into().unwrap());
                let lifetime = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
                requests
                    .lock()
                    .unwrap()
                    .push((internal, external, lifetime));
                let mut reply = vec![0, 130, 0, 0, 0, 0, 0, 1];
                reply.extend_from_slice(&bytes[4..12]);
                if lifetime != 0 {
                    if !server_enabled.load(Ordering::SeqCst) {
                        continue;
                    }
                    reply[10..12]
                        .copy_from_slice(&server_assigned.load(Ordering::SeqCst).to_be_bytes());
                    reply[12..16]
                        .copy_from_slice(&server_lifetime.load(Ordering::SeqCst).to_be_bytes());
                }
                reply
            };
            gateway.send_to(&reply, from).await.unwrap();
        }
    });
    let mut node = Node::new();
    node.stop_worker();
    let mut config = worker_owner::worker_config();
    let mapping = upnp::Config {
        network: upnp::Settings {
            ssdp,
            gateways: vec![address],
            ..Default::default()
        },
        system_gateways: false,
        ..Default::default()
    };
    let source = mapping.ports.clone();
    config.upnp = Some(mapping);
    let (mut workers, service) = workers::create(
        node.db.clone(),
        NetworkProfile::mainnet(),
        config,
        Default::default(),
    )
    .unwrap();
    let workers_task = tokio::spawn(service.run());
    within(async {
        while !workers.status().ready {
            assert!(workers.status().running);
            workers.changed().await.unwrap();
        }
    })
    .await;
    let bindings = workers.bindings();
    let binding = bindings.upnp.as_ref().unwrap();
    let old = binding.current().unwrap();
    assert_eq!(old.refresh().unwrap().await, Err(upnp::Error::NotReady));
    assert!(records.lock().unwrap().is_empty());
    assert_eq!(old.info().attempts, 0);
    let mut config = connection_owner::settings();
    config.peer.port = 4015;
    let (mut owner, service) = owner::create_with_workers(
        worker_owner::inputs(&node, &bindings),
        config.clone(),
        || Ok(Default::default()),
        bindings.clone(),
    )
    .unwrap();
    // The publication lease rejects a second owner of these mapping ports.
    assert!(matches!(
        owner::create_with_workers(
            worker_owner::inputs(&node, &bindings),
            config,
            || Ok(Default::default()),
            bindings.clone()
        ),
        Err(owner::Error::Configuration)
    ));
    let owner_task = tokio::spawn(service.run());
    let first = connection_owner::ready(&mut owner, 1).await;
    let first_port = first.network.status().listening.unwrap().port();
    assert_eq!(source.get().internal, 3015);
    let advertisement = bindings.upnp_listener.as_ref().unwrap();
    let (remote, _events, driver) = connection_owner::remote(&node, &first).await;
    advertised(&remote, &node, 1, 4015).await;
    enabled.store(true, Ordering::SeqCst);
    old.refresh().unwrap().await.unwrap();
    mapped(&old, first_port, 5015).await;
    advertised(&remote, &node, 1, 5015).await;
    advertised(&remote, &node, 2, 5015).await;
    enabled.store(false, Ordering::SeqCst);
    assert_eq!(
        workers
            .restart(workers::Child::Upnp, 1)
            .unwrap()
            .wait()
            .await,
        Ok(2)
    );
    assert!(old.info().joined && matches!(old.refresh(), Err(upnp::Error::Closed)));
    let next = binding.current().unwrap();
    assert_eq!(advertisement.advertised_port(4015), 4015);
    advertised(&remote, &node, 2, 4015).await;
    assigned.store(6015, Ordering::SeqCst);
    enabled.store(true, Ordering::SeqCst);
    next.refresh().unwrap().await.unwrap();
    mapped(&next, first_port, 6015).await;
    assert!(!remote.is_closed());
    assert_eq!(
        (owner.status().starts, owner.status().restart_attempts),
        (1, 0)
    );
    advertised(&remote, &node, 2, 6015).await;
    lifetime.store(1, Ordering::SeqCst);
    assigned.store(6115, Ordering::SeqCst);
    next.refresh().unwrap().await.unwrap();
    mapped(&next, first_port, 6115).await;
    advertised(&remote, &node, 2, 6115).await;
    within(async {
        while advertisement.advertised_port(4015) != 4015 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    advertised(&remote, &node, 2, 4015).await;
    lifetime.store(upnp::MAPPING_LIFETIME, Ordering::SeqCst);
    assigned.store(7015, Ordering::SeqCst);
    first.peer.shutdown();
    let second = connection_owner::ready(&mut owner, 2).await;
    within(driver).await.unwrap();
    connection_owner::closed(&first);
    let second_port = second.network.status().listening.unwrap().port();
    mapped(&next, second_port, 7015).await;
    let (remote, _events, driver) = connection_owner::remote(&node, &second).await;
    advertised(&remote, &node, 1, 7015).await;
    owner.shutdown();
    assert!(within(owner_task).await.unwrap().joined);
    within(driver).await.unwrap();
    connection_owner::closed(&second);
    assert_eq!(next.refresh().unwrap().await, Err(upnp::Error::NotReady));
    assert_eq!(advertisement.advertised_port(4015), 4015);
    workers.shutdown();
    let status = within(workers_task).await.unwrap();
    assert!(status.joined && next.info().joined && next.info().prepared);
    assert_eq!(status.upnp_cleanup_error, None);
    assert_eq!(source.retained_mappings(), 0);
    stop.send_replace(true);
    within(server).await.unwrap();
    let records = records.lock().unwrap();
    for port in [first_port, second_port] {
        assert!(records.contains(&(port, 4015, upnp::MAPPING_LIFETIME)));
        assert!(records.contains(&(port, 0, 0)));
    }
    assert!(
        records
            .iter()
            .all(|(internal, _, _)| [first_port, second_port].contains(internal))
    );
}

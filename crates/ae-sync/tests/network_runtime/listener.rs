use super::*;

async fn counts(h: &mut network::Handle, active: usize, sleeping: usize) {
    status(h, |s| {
        s.listener
            .is_some_and(|l| l.active == active && l.sleeping == sleeping)
            && s.sockets == active
    })
    .await;
}

#[tokio::test]
async fn ranch_threshold_overshoot_and_sleeper_wakeup_drive_actual_tcp_accepts() {
    let node = Node::new();
    let (local, _) = identities();
    let mut c = config();
    c.listener = network::listener::Config {
        acceptors: 3,
        maximum_connections: 2,
    };
    c.transport.runtime.handshake_timeout = Duration::from_secs(30);
    c.registry.policy.admission_timeout = 30_000;
    let (mut h, _events, _incoming, task) = start(&node, local, c).await;
    let address = h.status().listening.unwrap();
    let mut sockets = Vec::new();
    for n in 1..=4 {
        sockets.push(TcpStream::connect(address).await.unwrap());
        counts(&mut h, n, n.saturating_sub(1)).await;
    }
    let queued = TcpStream::connect(address).await.unwrap();
    // Crossing the threshold pauses acceptors; it does not reject TCP or
    // pretend that the permanent-peer admission limit owns these slots.
    assert_eq!(h.status().policy.as_ref().unwrap().counts.inbound, 0);
    drop(sockets.pop());
    counts(&mut h, 3, 3).await;
    drop(sockets.pop());
    counts(&mut h, 2, 3).await;
    drop(sockets.pop());
    // The third exit wakes a sleeper, which accepts the already queued socket.
    status(&mut h, |s| s.counts.rejected >= 3).await;
    counts(&mut h, 2, 3).await;
    drop(sockets.pop());
    counts(&mut h, 1, 2).await;
    drop(queued);
    counts(&mut h, 0, 1).await;
    stop(&h, task).await;
}

#[tokio::test]
async fn zero_threshold_counts_initial_acceptors_and_zero_acceptors_never_start_transport() {
    for acceptors in [0, 2] {
        let node = Node::new();
        let (local, _) = identities();
        let mut c = config();
        c.listener = network::listener::Config {
            acceptors,
            maximum_connections: 0,
        };
        let (mut h, _events, _incoming, task) = start(&node, local, c).await;
        let address = h.status().listening.unwrap();
        let mut sockets = Vec::new();
        for n in 1..=acceptors {
            sockets.push(TcpStream::connect(address).await.unwrap());
            counts(&mut h, n, n).await;
        }
        let queued = TcpStream::connect(address).await.unwrap();
        drop(sockets);
        counts(&mut h, 0, acceptors).await;
        let exit = stop(&h, task).await;
        assert_eq!(exit.status.listener.unwrap().sleeping, acceptors);
        drop(queued);
    }
}

#[tokio::test]
async fn noise_failure_and_first_ping_expiry_release_listener_lifetimes() {
    for authenticated in [false, true] {
        let node = Node::new();
        let (local, remote) = identities();
        let mut c = config();
        c.listener = network::listener::Config {
            acceptors: 1,
            maximum_connections: 1,
        };
        c.transport.runtime.handshake_timeout = Duration::from_millis(500);
        c.transport.first_ping_timeout = Duration::from_millis(500);
        c.registry.policy.admission_timeout = 10_000;
        let (mut h, _events, _incoming, task) = start(&node, local.clone(), c).await;
        if authenticated {
            let (_client, _events, driver) = incoming(&node, &h, local.public_key(), &remote).await;
            counts(&mut h, 1, 1).await;
            within(driver).await.unwrap();
        } else {
            let socket = TcpStream::connect(h.status().listening.unwrap())
                .await
                .unwrap();
            counts(&mut h, 1, 1).await;
            eof(&socket).await;
        }
        counts(&mut h, 0, 0).await;
        // A fresh connection proves the wakeup reached the real socket owner.
        let next = TcpStream::connect(h.status().listening.unwrap())
            .await
            .unwrap();
        counts(&mut h, 1, 1).await;
        stop(&h, task).await;
        eof(&next).await;
    }
}

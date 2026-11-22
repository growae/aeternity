use super::{Error, Settings};
use std::{
    collections::BTreeSet,
    net::{Ipv4Addr, SocketAddrV4},
};

pub(super) fn system(mut settings: Settings) -> Result<Settings, Error> {
    settings.validate()?;
    settings.gateways = select(netdev::get_interfaces(), settings.bind)?;
    Ok(settings)
}
fn usable(ip: Ipv4Addr) -> bool {
    !ip.is_unspecified() && !ip.is_loopback() && !ip.is_multicast() && !ip.is_broadcast()
}
fn select(interfaces: Vec<netdev::Interface>, bind: Ipv4Addr) -> Result<Vec<SocketAddrV4>, Error> {
    let mut gateways = BTreeSet::new();
    let mut fallback = BTreeSet::new();
    for interface in interfaces {
        if !bind.is_unspecified() && !interface.ipv4.iter().any(|n| n.addr() == bind) {
            continue;
        }
        if let Some(gateway) = interface.gateway {
            gateways.extend(gateway.ipv4.into_iter().filter(|ip| usable(*ip)));
        }
        for subnet in interface.ipv4 {
            if subnet.addr().is_private() && subnet.prefix_len() <= 30 {
                let candidate = Ipv4Addr::from(u32::from(subnet.network()) | 1);
                if candidate != subnet.addr() && usable(candidate) {
                    fallback.insert(candidate);
                }
            }
        }
    }
    // Match erlang-nat's preference for configured gateways. Its fallback
    // probes only the first host of each private subnet, not every address.
    if gateways.is_empty() {
        gateways = fallback;
    }
    if gateways.len() > 32 {
        return Err(Error::Limit);
    }
    Ok(gateways
        .into_iter()
        .map(|ip| SocketAddrV4::new(ip, 5351))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn interface(address: &str, gateway: Option<&str>) -> netdev::Interface {
        let mut i = netdev::Interface::dummy();
        i.ipv4.push(address.parse().unwrap());
        i.gateway = gateway.map(|g| {
            let mut device = netdev::NetworkDevice::new();
            device.ipv4.push(g.parse().unwrap());
            device
        });
        i
    }
    #[test]
    fn configured_routes_precede_bounded_private_subnet_candidates() {
        let configured = interface("192.168.2.4/24", Some("192.168.2.254"));
        let fallback = interface("10.2.0.5/16", None);
        assert_eq!(
            select(
                vec![configured.clone(), configured.clone(), fallback.clone()],
                Ipv4Addr::UNSPECIFIED
            )
            .unwrap(),
            vec!["192.168.2.254:5351".parse().unwrap()]
        );
        assert_eq!(
            select(
                vec![configured, fallback.clone()],
                "10.2.0.5".parse().unwrap()
            )
            .unwrap(),
            vec!["10.2.0.1:5351".parse().unwrap()]
        );
        assert!(
            select(
                vec![
                    interface("192.0.2.5/24", None),
                    interface("10.0.0.1/24", None),
                    interface("10.0.0.7/32", None)
                ],
                Ipv4Addr::UNSPECIFIED
            )
            .unwrap()
            .is_empty()
        );
        let many = (0..33)
            .map(|n| interface(&format!("10.{n}.0.3/16"), None))
            .collect();
        assert_eq!(select(many, Ipv4Addr::UNSPECIFIED), Err(Error::Limit));
    }
}

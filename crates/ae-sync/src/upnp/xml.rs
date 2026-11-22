use super::{Error, Ports, Settings, http};
use roxmltree::{Document, Node, ParsingOptions};

const SOAP: &str = "http://schemas.xmlsoap.org/soap/envelope/";
pub(super) fn service(version: u8) -> String {
    format!("urn:schemas-upnp-org:service:WANIPConnection:{version}")
}
pub(super) fn target(version: u8) -> String {
    format!("urn:schemas-upnp-org:device:InternetGatewayDevice:{version}")
}
pub(super) fn document<'a>(raw: &'a [u8], s: &Settings) -> Result<Document<'a>, Error> {
    if raw.len() > s.body_bytes {
        return Err(Error::Limit);
    }
    Document::parse_with_options(
        std::str::from_utf8(raw).map_err(|_| Error::Xml)?,
        ParsingOptions {
            allow_dtd: false,
            nodes_limit: s.xml_nodes,
            entity_resolver: None,
        },
    )
    .map_err(|_| Error::Xml)
}
fn child<'a>(n: Node<'a, 'a>, name: &str) -> Result<Node<'a, 'a>, Error> {
    n.children()
        .find(|n| n.has_tag_name(name))
        .ok_or(Error::Xml)
}
fn text<'a>(n: Node<'a, 'a>, name: &str) -> Result<&'a str, Error> {
    let n = child(n, name)?;
    if n.children().any(|n| n.is_element()) {
        return Err(Error::Xml);
    }
    n.text().map(str::trim).ok_or(Error::Xml)
}
fn device<'a>(n: Node<'a, 'a>, kind: &str, version: u8) -> Result<Node<'a, 'a>, Error> {
    let expected = format!("urn:schemas-upnp-org:device:{kind}:{version}");
    child(n, "deviceList")?
        .children()
        .find(|n| n.has_tag_name("device") && text(*n, "deviceType") == Ok(expected.as_str()))
        .ok_or(Error::Xml)
}
pub(super) fn control(
    raw: &[u8],
    root: &http::Url,
    discovery_version: u8,
    settings: &Settings,
) -> Result<(http::Url, u8), Error> {
    let doc = document(raw, settings)?;
    let gateway = child(doc.root_element(), "device")?;
    let kind = text(gateway, "deviceType")?;
    let version = if kind == target(1) {
        1
    } else if kind == target(2) && discovery_version == 2 {
        2
    } else {
        return Err(Error::Xml);
    };
    let wan = device(gateway, "WANDevice", version)?;
    let connection = device(wan, "WANConnectionDevice", version)?;
    let expected = service(version);
    let entry = child(connection, "serviceList")?
        .children()
        .find(|n| n.has_tag_name("service") && text(*n, "serviceType") == Ok(expected.as_str()))
        .ok_or(Error::Xml)?;
    Ok((root.control(text(entry, "controlURL")?)?, version))
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Action {
    Add { ports: Ports, lifetime: u32 },
    Delete { external: u16 },
    NatStatus,
    Specific { external: u16 },
    Generic { index: u16 },
}
impl Action {
    pub fn name(self, version: u8) -> &'static str {
        match self {
            Self::Add { .. } if version == 2 => "AddAnyPortMapping",
            Self::Add { .. } => "AddPortMapping",
            Self::Delete { .. } => "DeletePortMapping",
            Self::NatStatus => "GetNATRSIPStatus",
            Self::Specific { .. } => "GetSpecificPortMappingEntry",
            Self::Generic { .. } => "GetGenericPortMappingEntry",
        }
    }
}
pub(super) fn request(action: Action, version: u8, ip: std::net::Ipv4Addr) -> (String, String) {
    request_named(action, version, ip, None)
}
pub(super) fn request_named(
    action: Action,
    version: u8,
    ip: std::net::Ipv4Addr,
    description: Option<&str>,
) -> (String, String) {
    let name = action.name(version);
    let ns = service(version);
    let arguments = match action {
        Action::Add { ports, lifetime } => {
            let default = format!("{ip}_TCP_{}", ports.internal);
            let description = description.unwrap_or(&default);
            format!(
                "<NewRemoteHost></NewRemoteHost><NewExternalPort>{}</NewExternalPort><NewProtocol>TCP</NewProtocol><NewInternalPort>{}</NewInternalPort><NewInternalClient>{ip}</NewInternalClient><NewEnabled>1</NewEnabled><NewPortMappingDescription>{description}</NewPortMappingDescription><NewLeaseDuration>{lifetime}</NewLeaseDuration>",
                ports.external, ports.internal
            )
        }
        Action::Delete { external } | Action::Specific { external } => format!(
            "<NewRemoteHost></NewRemoteHost><NewExternalPort>{external}</NewExternalPort><NewProtocol>TCP</NewProtocol>"
        ),
        Action::NatStatus => String::new(),
        Action::Generic { index } => format!("<NewPortMappingIndex>{index}</NewPortMappingIndex>"),
    };
    (
        format!("{ns}#{name}"),
        format!(
            "<?xml version=\"1.0\"?><s:Envelope xmlns:s=\"{SOAP}\" s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body><u:{name} xmlns:u=\"{ns}\">{arguments}</u:{name}></s:Body></s:Envelope>"
        ),
    )
}

pub(super) struct Entry {
    pub internal: u16,
    pub external: u16,
    pub client: String,
    pub description: String,
    pub tcp: bool,
    pub wildcard: bool,
}
pub(super) fn entry(
    response: &http::Response,
    action: Action,
    version: u8,
    settings: &Settings,
) -> Result<Entry, Error> {
    self::response(response, action, version, settings)?;
    let doc = document(&response.body, settings)?;
    let name = format!("{}Response", action.name(version));
    let result = doc
        .root_element()
        .children()
        .find(|n| n.has_tag_name((SOAP, "Body")))
        .and_then(|n| {
            n.children()
                .find(|n| n.has_tag_name((service(version).as_str(), name.as_str())))
        })
        .ok_or(Error::Xml)?;
    let (external, tcp, wildcard) = match action {
        Action::Specific { external } => (external, true, true),
        Action::Generic { .. } => {
            let external = text(result, "NewExternalPort")?
                .parse()
                .map_err(|_| Error::Response)?;
            let tcp = match text(result, "NewProtocol")? {
                "TCP" => true,
                "UDP" => false,
                _ => return Err(Error::Response),
            };
            let host = child(result, "NewRemoteHost")?;
            if host.children().any(|n| n.is_element()) {
                return Err(Error::Xml);
            }
            (external, tcp, host.text().unwrap_or("").trim().is_empty())
        }
        _ => return Err(Error::Response),
    };
    let description_node = child(result, "NewPortMappingDescription")?;
    if description_node.children().any(|n| n.is_element()) {
        return Err(Error::Xml);
    }
    let description = description_node.text().unwrap_or("").trim();
    if description.len() > 1024 {
        return Err(Error::Limit);
    }
    // InternalClient is a string in the service schema. Foreign entries may
    // contain hostnames; they must not prevent recovery of our numeric entry.
    let client = text(result, "NewInternalClient")?;
    if client.len() > 1024 {
        return Err(Error::Limit);
    }
    Ok(Entry {
        internal: text(result, "NewInternalPort")?
            .parse()
            .map_err(|_| Error::Response)?,
        external,
        client: client.to_owned(),
        description: description.to_owned(),
        tcp,
        wildcard,
    })
}
pub(super) fn response(
    response: &http::Response,
    action: Action,
    version: u8,
    settings: &Settings,
) -> Result<Option<u16>, Error> {
    // UPnP v1 add/delete acknowledgements may carry an empty body.
    if response.status == 200
        && response.body.is_empty()
        && matches!(action, Action::Delete { .. } | Action::Add { .. })
        && (version == 1 || matches!(action, Action::Delete { .. }))
    {
        return Ok(None);
    }
    let doc = match document(&response.body, settings) {
        Ok(doc) => doc,
        Err(_) if response.status != 200 => return Err(Error::HttpStatus(response.status)),
        Err(e) => return Err(e),
    };
    let envelope = doc.root_element();
    if !envelope.has_tag_name((SOAP, "Envelope")) {
        return Err(Error::Xml);
    }
    let body = envelope
        .children()
        .find(|n| n.has_tag_name((SOAP, "Body")))
        .ok_or(Error::Xml)?;
    if let Some(fault) = body.children().find(|n| n.has_tag_name((SOAP, "Fault"))) {
        let detail = child(fault, "detail")?;
        let error = child(detail, "UPnPError")?;
        let code = text(error, "errorCode")?
            .parse::<u16>()
            .map_err(|_| Error::Xml)?;
        return Err(Error::Soap(code));
    }
    if response.status != 200 {
        return Err(Error::HttpStatus(response.status));
    }
    let name = format!("{}Response", action.name(version));
    let ns = service(version);
    let result = body
        .children()
        .find(|n| n.has_tag_name((ns.as_str(), name.as_str())))
        .ok_or(Error::Xml)?;
    match action {
        Action::Add { .. } if version == 2 => {
            let port = text(result, "NewReservedPort")?
                .parse::<u16>()
                .map_err(|_| Error::Response)?;
            if port == 0 {
                return Err(Error::Response);
            }
            Ok(Some(port))
        }
        Action::NatStatus => match text(result, "NewNATEnabled")? {
            "1" | "true" => Ok(Some(1)),
            "0" | "false" => Ok(Some(0)),
            _ => Err(Error::Response),
        },
        _ => Ok(None),
    }
}

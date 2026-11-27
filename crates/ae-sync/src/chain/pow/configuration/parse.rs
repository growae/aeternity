use super::*;
use serde_json::Map;

fn object<'a>(value: &'a Value, field: &'static str) -> Result<&'a Map<String, Value>, Error> {
    value.as_object().ok_or(Error::Field(field))
}
fn integer(value: &Value, field: &'static str) -> Result<BigInt, Error> {
    let text = value.as_number().ok_or(Error::Field(field))?.to_string();
    if text.len() > 4096 {
        return Err(Error::Limit("integer"));
    }
    text.parse().map_err(|_| Error::Field(field))
}
fn string(
    map: &Map<String, Value>,
    field: &'static str,
    default: Option<&str>,
) -> Result<String, Error> {
    let text = map
        .get(field)
        .map_or(default, Value::as_str)
        .ok_or(Error::Field(field))?;
    if text.len() > MAX_TEXT {
        return Err(Error::Limit("miner text"));
    }
    Ok(text.into())
}
fn definition(value: &Value) -> Result<Miner, Error> {
    let map = object(value, "miner")?;
    let result = Miner {
        executable: string(map, "executable", None)?,
        executable_group: string(map, "executable_group", Some("aecuckoo"))?,
        extra_args: string(map, "extra_args", Some(""))?,
        hex_encoded_header: map.get("hex_encoded_header").map_or(Ok(false), |v| {
            v.as_bool().ok_or(Error::Field("hex_encoded_header"))
        })?,
        repeats: map
            .get("repeats")
            .map_or(Ok(1.into()), |v| integer(v, "repeats"))?,
        addressed_instances: map
            .get("addressed_instances")
            .map(|v| {
                let ids = v.as_array().ok_or(Error::Field("addressed_instances"))?;
                if ids.len() > MAX_INSTANCES {
                    return Err(Error::Limit("instances"));
                }
                ids.iter()
                    .map(|id| integer(id, "addressed_instances"))
                    .collect()
            })
            .transpose()?,
    };
    result.validate()?;
    Ok(result)
}
pub(super) fn system_miners(value: &Value) -> Result<Vec<Miner>, Error> {
    let rows = value.as_array().ok_or(Error::Field("system miners"))?;
    if rows.len() > MAX_DEFINITIONS {
        return Err(Error::Limit("miners"));
    }
    rows.iter()
        .map(|row| {
            let fields = row
                .as_array()
                .filter(|row| row.len() == 6)
                .ok_or(Error::Field("system miner"))?;
            let mut map: Map<String, Value> = [
                ("executable", &fields[0]),
                ("extra_args", &fields[1]),
                ("hex_encoded_header", &fields[2]),
                ("repeats", &fields[3]),
                ("executable_group", &fields[5]),
            ]
            .into_iter()
            .map(|(k, v)| (k.into(), v.clone()))
            .collect();
            // null represents the native undefined instance list.
            if !fields[4].is_null() {
                map.insert("addressed_instances".into(), fields[4].clone());
            }
            definition(&Value::Object(map))
        })
        .collect()
}
pub(super) fn normalize(value: &Value, system: &System) -> Result<Configuration, Error> {
    let map = object(value, "cuckoo")?;
    if map.contains_key("miner") && (map.contains_key("miners") || map.contains_key("edge_bits")) {
        return Err(Error::Conflict);
    }
    let edge_bits = map
        .get("edge_bits")
        .map_or_else(|| Ok(system.edge_bits.clone()), |v| integer(v, "edge_bits"))?;
    let mut miners = if let Some(modern) = map.get("miners") {
        let definitions = modern.as_array().ok_or(Error::Field("miners"))?;
        if definitions.len() > MAX_DEFINITIONS {
            return Err(Error::Limit("miners"));
        }
        definitions
            .iter()
            .map(definition)
            .collect::<Result<Vec<_>, _>>()?
    } else if let Some(deprecated) = map.get("miner") {
        let mut value = deprecated.clone();
        if let Some(count) = object(deprecated, "miner")?.get("instances") {
            let count = integer(count, "instances")?;
            if count < BigInt::from(0) {
                return Err(Error::Field("instances"));
            }
            let count = usize::try_from(&count)
                .ok()
                .filter(|n| *n <= MAX_INSTANCES)
                .ok_or(Error::Limit("instances"))?;
            value.as_object_mut().expect("checked object").insert(
                "addressed_instances".into(),
                Value::Array((0..count).map(Value::from).collect()),
            );
        }
        // Deprecated nested edge_bits is not read by aec_mining.
        vec![definition(&value)?]
    } else {
        if system.miners.len() > MAX_DEFINITIONS {
            return Err(Error::Limit("miners"));
        }
        system.miners.clone()
    };
    miners.reverse();
    let result = Configuration { edge_bits, miners };
    result.validate()?;
    result.instances()?;
    Ok(result)
}
fn fields(map: &Map<String, Value>, allowed: &[&str]) -> Result<(), Error> {
    if map.keys().any(|key| !allowed.contains(&key.as_str())) {
        Err(Error::Field("unknown field"))
    } else {
        Ok(())
    }
}
pub(super) fn schema(value: &Value) -> Result<(), Error> {
    let map = object(value, "cuckoo")?;
    fields(map, &["edge_bits", "miners", "miner"])?;
    if let Some(value) = map.get("edge_bits") {
        integer(value, "edge_bits")?;
    }
    for (name, deprecated) in [("miners", false), ("miner", true)] {
        let Some(value) = map.get(name) else {
            continue;
        };
        let rows = if deprecated {
            std::slice::from_ref(value)
        } else {
            value.as_array().ok_or(Error::Field("miners"))?.as_slice()
        };
        if rows.len() > MAX_DEFINITIONS {
            return Err(Error::Limit("miners"));
        }
        for row in rows {
            let row = object(row, "miner")?;
            let mut allowed = vec![
                "executable",
                "executable_group",
                "extra_args",
                "hex_encoded_header",
                "nice",
                "repeats",
            ];
            if deprecated {
                allowed.extend(["edge_bits", "instances"]);
            } else {
                allowed.push("addressed_instances");
            }
            fields(row, &allowed)?;
            string(row, "executable", None)?;
            string(row, "extra_args", Some(""))?;
            if let Some(value) = row.get("hex_encoded_header")
                && !value.is_boolean()
            {
                return Err(Error::Field("hex_encoded_header"));
            }
            if let Some(value) = row.get("repeats") {
                integer(value, "repeats")?;
            }
            if deprecated {
                string(row, "extra_args", None)?;
                integer(
                    row.get("edge_bits").ok_or(Error::Field("edge_bits"))?,
                    "edge_bits",
                )?;
                if let Some(value) = row.get("instances") {
                    integer(value, "instances")?;
                }
            }
            if let Some(value) = row.get("nice") {
                integer(value, "nice")?;
            }
            if let Some(group) = row.get("executable_group")
                && !matches!(group.as_str(), Some("aecuckoo" | "aecuckooprebuilt"))
            {
                return Err(Error::Group);
            }
            if let Some(ids) = row.get("addressed_instances") {
                let ids = ids
                    .as_array()
                    .filter(|ids| !ids.is_empty())
                    .ok_or(Error::Field("addressed_instances"))?;
                if ids.len() > MAX_INSTANCES {
                    return Err(Error::Limit("instances"));
                }
                for id in ids {
                    integer(id, "addressed_instances")?;
                }
            }
        }
    }
    Ok(())
}

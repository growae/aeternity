use super::*;
use ae_vm_fate::{
    Limits,
    maps::{Traversal, order::binary_indices},
};
use std::rc::Rc;

#[derive(Clone, PartialEq, Eq)]
pub(super) enum Visit {
    Setting(String),
    Variable(String),
}
#[derive(Clone, Copy)]
enum Builtin {
    Home,
    Data,
    Log,
    App,
    Library,
    Private,
}
enum Entry<'a> {
    Builtin(&'static str, Builtin, &'a str),
    Custom(&'a Definition, &'a str),
}
struct Link<'a> {
    entry: Entry<'a>,
    next: Scope<'a>,
}
type Scope<'a> = Option<Rc<Link<'a>>>;
pub(super) struct State<'a> {
    context: &'a Context,
    nodes: usize,
    bytes: usize,
}
impl<'a> State<'a> {
    pub(super) fn new(context: &'a Context) -> Self {
        Self {
            context,
            nodes: 0,
            bytes: 0,
        }
    }
    fn charge(&mut self, nodes: usize, bytes: usize) -> Result<(), Error> {
        self.nodes = self
            .nodes
            .checked_add(nodes)
            .filter(|n| *n <= MAX_NODES)
            .ok_or(Error::Limit)?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|n| *n <= MAX_BYTES)
            .ok_or(Error::Limit)?;
        Ok(())
    }
    fn scope(&mut self, app: &'a str) -> Result<Scope<'a>, Error> {
        let mut scope = None;
        for (name, kind) in [
            ("HOME", Builtin::Home),
            ("LOG_DIR", Builtin::Log),
            ("DATA_DIR", Builtin::Data),
        ] {
            scope = Some(Rc::new(Link {
                entry: Entry::Builtin(name, kind, "setup"),
                next: scope,
            }));
        }
        for d in &self.context.global {
            scope = Some(Rc::new(Link {
                entry: Entry::Custom(d, "setup"),
                next: scope,
            }));
        }
        for (name, kind) in [
            ("LIB_DIR", Builtin::Library),
            ("PRIV_DIR", Builtin::Private),
            ("APP", Builtin::App),
        ] {
            scope = Some(Rc::new(Link {
                entry: Entry::Builtin(name, kind, app),
                next: scope,
            }));
        }
        if let Some(defs) = self.context.private.get(app) {
            for d in defs {
                scope = Some(Rc::new(Link {
                    entry: Entry::Custom(d, app),
                    next: scope,
                }));
            }
        }
        self.charge(
            6 + self.context.global.len() + self.context.private.get(app).map_or(0, Vec::len),
            0,
        )?;
        Ok(scope)
    }
    pub(super) fn value(
        &mut self,
        app: &'a str,
        value: &Value,
        visited: &[Visit],
        depth: usize,
    ) -> Result<Value, Error> {
        let scope = self.scope(app)?;
        self.walk(app, value, &scope, visited, depth)
    }
    fn walk(
        &mut self,
        app: &'a str,
        value: &Value,
        scope: &Scope<'a>,
        visited: &[Visit],
        depth: usize,
    ) -> Result<Value, Error> {
        if depth > MAX_DEPTH {
            return Err(Error::Limit);
        }
        self.charge(1, 8)?;
        match value {
            Value::String(s) => {
                String::from_utf8(self.string(app, s.as_bytes(), scope, visited, depth + 1)?)
                    .map(Value::String)
                    .map_err(|_| Error::Encoding)
            }
            Value::Array(a) if byte_list(a) => {
                let bytes: Vec<u8> = a
                    .iter()
                    .map(|v| {
                        v.as_u64()
                            .and_then(|n| u8::try_from(n).ok())
                            .ok_or(Error::Encoding)
                    })
                    .collect::<Result<_, _>>()?;
                let expanded = self.string(app, &bytes, scope, visited, depth + 1)?;
                self.charge(expanded.len(), 0)?;
                Ok(Value::Array(
                    expanded.into_iter().map(Value::from).collect(),
                ))
            }
            Value::Array(a) => a
                .iter()
                .map(|v| self.walk(app, v, scope, visited, depth + 1))
                .collect::<Result<_, _>>()
                .map(Value::Array),
            Value::Object(m) => {
                let entries: Vec<_> = m.iter().collect();
                let keys: Vec<_> = entries.iter().map(|(k, _)| k.as_bytes()).collect();
                let order = binary_indices(
                    &keys,
                    Traversal::ToList,
                    Limits {
                        max_nodes: MAX_NODES,
                        max_bytes: MAX_BYTES,
                        max_depth: MAX_DEPTH,
                    },
                )
                .map_err(|_| Error::Limit)?;
                self.charge(m.len(), keys.iter().map(|k| k.len()).sum())?;
                let mut result = serde_json::Map::new();
                for i in order {
                    let (k, v) = entries[i];
                    let key = String::from_utf8(self.string(
                        app,
                        k.as_bytes(),
                        scope,
                        visited,
                        depth + 1,
                    )?)
                    .map_err(|_| Error::Encoding)?;
                    let value = self.walk(app, v, scope, visited, depth + 1)?;
                    result.insert(key, value);
                }
                Ok(Value::Object(result))
            }
            _ => {
                self.charge(0, value.as_number().map_or(0, |n| n.as_str().len()))?;
                Ok(value.clone())
            }
        }
    }
    fn append(&mut self, out: &mut Vec<u8>, s: &[u8]) -> Result<(), Error> {
        self.charge(0, s.len())?;
        out.extend_from_slice(s);
        Ok(())
    }
    fn string(
        &mut self,
        app: &'a str,
        text: &[u8],
        scope: &Scope<'a>,
        visited: &[Visit],
        depth: usize,
    ) -> Result<Vec<u8>, Error> {
        if depth > MAX_DEPTH {
            return Err(Error::Limit);
        }
        self.charge(1, text.len())?;
        let mut out = Vec::new();
        let mut rest = text;
        while let Some(pos) = rest.iter().position(|b| *b == b'$') {
            self.append(&mut out, &rest[..pos])?;
            rest = &rest[pos + 1..];
            if let Some(tail) = rest.strip_prefix(b"env(") {
                let parsed = tail.iter().position(|b| *b == b')').and_then(|end| {
                    let name: String = tail[..end].iter().copied().map(char::from).collect();
                    self.context
                        .known_names
                        .contains(&name)
                        .then_some((name, end + 1))
                });
                if let Some((name, consumed)) = parsed {
                    if let Some(value) = self
                        .context
                        .applications
                        .get(app)
                        .and_then(|m| m.get(&name))
                    {
                        let mark = Visit::Setting(name);
                        if visited.contains(&mark) {
                            return Err(Error::Cycle);
                        }
                        let mut next = visited.to_vec();
                        next.push(mark);
                        let value = self.value(app, value, &next, depth + 1)?;
                        // setup expands $env results once more in the caller's
                        // variable scope, so literal custom variables can differ.
                        let value = self.walk(app, &value, scope, visited, depth + 1)?;
                        let bytes = text::stringify(&value)?;
                        self.append(&mut out, &bytes)?;
                        rest = &tail[consumed..];
                        continue;
                    }
                    self.append(&mut out, b"$env(")?;
                }
                // Unknown atoms or missing ')' discard only the '$env(' prefix;
                // a known but unset name keeps it. The remainder is still scanned.
                rest = tail;
                continue;
            }
            let mut cursor = scope.clone();
            let mut matched = false;
            while let Some(link) = cursor {
                self.charge(1, 0)?;
                let consumed = match &link.entry {
                    Entry::Builtin(name, _, _) => {
                        rest.starts_with(name.as_bytes()).then_some(name.len())
                    }
                    Entry::Custom(d, _) => {
                        let latin1: String = rest.iter().copied().map(char::from).collect();
                        self.charge(0, latin1.len())?;
                        // re:split ignores the empty match at the beginning.
                        let mut matches = d.pattern.captures_iter(&latin1).filter(|c| {
                            !c.as_ref().is_ok_and(|c| {
                                c.get(0).is_some_and(|m| m.start() == 0 && m.end() == 0)
                            })
                        });
                        match matches
                            .next()
                            .transpose()
                            .map_err(|_| Error::VariablePattern)?
                        {
                            None => None,
                            Some(c) => {
                                let m = c.get(0).ok_or(Error::VariablePattern)?;
                                if m.start() != 0 || c.len() != 1 || matches.next().is_some() {
                                    return Err(Error::VariablePattern);
                                }
                                Some(latin1[..m.end()].chars().count())
                            }
                        }
                    }
                };
                if let Some(n) = consumed {
                    let value = match &link.entry {
                        Entry::Builtin(_, kind, owner) => {
                            self.builtin(*kind, owner, visited, depth + 1)?
                        }
                        Entry::Custom(d, owner) => match &d.value {
                            Variable::Literal(v) => {
                                let (n, b, _) = budget(v).map_err(|_| Error::Limit)?;
                                self.charge(n, b)?;
                                v.clone()
                            }
                            Variable::Expand(v) => {
                                let mark = Visit::Variable(d.name.clone());
                                if visited.contains(&mark) {
                                    return Err(Error::Cycle);
                                }
                                let mut next = visited.to_vec();
                                next.push(mark);
                                self.walk(owner, v, &link.next, &next, depth + 1)?
                            }
                        },
                    };
                    let bytes = text::stringify(&value)?;
                    self.append(&mut out, &bytes)?;
                    rest = &rest[n..];
                    matched = true;
                    break;
                }
                cursor = link.next.clone();
            }
            if !matched {
                self.append(&mut out, b"$")?;
            }
        }
        self.append(&mut out, rest)?;
        Ok(out)
    }
    fn builtin(
        &mut self,
        kind: Builtin,
        app: &'a str,
        visited: &[Visit],
        depth: usize,
    ) -> Result<Value, Error> {
        let (key, fallback) = match kind {
            Builtin::Home => ("home", &self.context.directories.home),
            Builtin::Log => ("log_dir", &self.context.directories.log),
            Builtin::Data => ("data_dir", &self.context.directories.data),
            Builtin::App => return Ok(Value::String(text::atom(app))),
            Builtin::Library | Builtin::Private => {
                let (l, p) = self.context.libraries.get(app).ok_or(Error::Directory)?;
                return Ok(Value::String(
                    if matches!(kind, Builtin::Library) {
                        l
                    } else {
                        p
                    }
                    .clone(),
                ));
            }
        };
        if let Some(v) = self
            .context
            .applications
            .get("setup")
            .and_then(|m| m.get(key))
        {
            let mark = Visit::Setting(key.into());
            if visited.contains(&mark) {
                return Err(Error::Cycle);
            }
            let mut next = visited.to_vec();
            next.push(mark);
            self.value("setup", v, &next, depth + 1)
        } else {
            self.charge(0, fallback.len())?;
            Ok(Value::String(fallback.clone()))
        }
    }
}
fn byte_list(a: &[Value]) -> bool {
    a.iter()
        .all(|v| v.as_f64().is_some_and(|f| (0.0..=255.0).contains(&f)))
}
pub(super) fn expandable(value: &Value) -> bool {
    match value {
        Value::String(s) => s.contains('$'),
        Value::Array(a) if byte_list(a) => a.iter().any(|v| v.as_u64() == Some(36)),
        Value::Array(a) => a.iter().any(expandable),
        Value::Object(m) => m.iter().any(|(k, v)| k.contains('$') || expandable(v)),
        _ => false,
    }
}

// Runtime lookup and expansion from aeu_env and setup 2.1.1. Resolution uses
// explicit caller-owned snapshots; no ambient environment or I/O is consulted.
use super::{Configuration, MAX_BYTES, MAX_DEPTH, MAX_NODES, budget, schema_default};
use fancy_regex::Regex;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::PathBuf,
};

mod expand;
mod pattern;
mod services;
mod text;
pub use services::Mode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Limit,
    Encoding,
    Cycle,
    VariablePattern,
    Directory,
    Lookup,
    Field(&'static str),
    Mining(ae_sync::chain::pow::configuration::Error),
    Hyperchain(ae_sync::chain::hyperchain::configuration::Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "configuration resolution error: {self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Clone)]
pub enum Variable {
    Literal(Value),
    Expand(Value),
}
pub struct Definition {
    name: String,
    pattern: Regex,
    value: Variable,
}
impl Definition {
    pub fn new(name: String, value: Variable) -> Result<Self, Error> {
        if name.len() > 1024 {
            return Err(Error::Limit);
        }
        let bytes_pattern = pattern::adapt(&name);
        let pattern = fancy_regex::RegexBuilder::new(&format!("^{bytes_pattern}"))
            .backtrack_limit(10_000)
            .delegate_size_limit(1 << 16)
            .delegate_dfa_size_limit(1 << 16)
            .build()
            .map_err(|_| Error::VariablePattern)?;
        let v = match &value {
            Variable::Literal(v) | Variable::Expand(v) => v,
        };
        budget(v).map_err(|_| Error::Limit)?;
        Ok(Self {
            name,
            pattern,
            value,
        })
    }
}
// Paths are resolved release resources, supplied by the enclosing node owner.
// They are distinct from the OS HOME used by file discovery.
#[derive(Clone)]
pub struct Directories {
    pub home: String,
    pub data: String,
    pub log: String,
}
pub struct Context {
    directories: Directories,
    applications: BTreeMap<String, BTreeMap<String, Value>>,
    known_names: BTreeSet<String>,
    global: Vec<Definition>,
    private: BTreeMap<String, Vec<Definition>>,
    libraries: BTreeMap<String, (String, String)>,
}
impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConfigurationContext { .. }")
    }
}
impl Context {
    pub fn new(directories: Directories) -> Result<Self, Error> {
        let result = Self {
            directories,
            applications: BTreeMap::new(),
            known_names: [
                "home",
                "data_dir",
                "log_dir",
                "true",
                "false",
                "undefined",
                "null",
            ]
            .map(String::from)
            .into(),
            global: Vec::new(),
            private: BTreeMap::new(),
            libraries: BTreeMap::new(),
        };
        result.validate()?;
        Ok(result)
    }
    // Existing atom membership affects setup's malformed/missing $env syntax.
    // Interning is explicit and bounded; removing a setting retains its name.
    pub fn intern(&mut self, name: String) -> Result<(), Error> {
        if self.known_names.contains(&name) {
            return Ok(());
        }
        if name.len() > 1024 || self.known_names.len() >= MAX_NODES {
            return Err(Error::Limit);
        }
        self.known_names.insert(name.clone());
        if let Err(e) = self.validate() {
            self.known_names.remove(&name);
            return Err(e);
        }
        Ok(())
    }
    pub fn set(&mut self, app: String, key: String, value: Value) -> Result<(), Error> {
        if app.len() > 1024 || key.len() > 1024 {
            return Err(Error::Limit);
        }
        budget(&value).map_err(|_| Error::Limit)?;
        let existed = self.known_names.contains(&key);
        self.intern(key.clone())?;
        let old = self
            .applications
            .entry(app.clone())
            .or_default()
            .insert(key.clone(), value);
        if let Err(error) = self.validate() {
            let m = self.applications.get_mut(&app).expect("inserted");
            if let Some(old) = old {
                m.insert(key.clone(), old);
            } else {
                m.remove(&key);
            }
            if m.is_empty() {
                self.applications.remove(&app);
            }
            if !existed {
                self.known_names.remove(&key);
            }
            return Err(error);
        }
        Ok(())
    }
    pub fn remove(&mut self, app: &str, key: &str) {
        if let Some(map) = self.applications.get_mut(app) {
            map.remove(key);
            if map.is_empty() {
                self.applications.remove(app);
            }
        }
    }
    pub fn set_global_variables(&mut self, definitions: Vec<Definition>) -> Result<(), Error> {
        let old = std::mem::replace(&mut self.global, definitions);
        if let Err(e) = self.validate() {
            self.global = old;
            return Err(e);
        }
        Ok(())
    }
    pub fn set_private_variables(
        &mut self,
        app: String,
        definitions: Vec<Definition>,
    ) -> Result<(), Error> {
        if app.len() > 1024 {
            return Err(Error::Limit);
        }
        let old = self.private.insert(app.clone(), definitions);
        if let Err(e) = self.validate() {
            if let Some(old) = old {
                self.private.insert(app, old);
            } else {
                self.private.remove(&app);
            }
            return Err(e);
        }
        Ok(())
    }
    pub fn set_library(
        &mut self,
        app: String,
        library: String,
        private: String,
    ) -> Result<(), Error> {
        if app.len() > 1024 || library.len() + private.len() > MAX_BYTES {
            return Err(Error::Limit);
        }
        let old = self.libraries.insert(app.clone(), (library, private));
        if let Err(e) = self.validate() {
            if let Some(old) = old {
                self.libraries.insert(app, old);
            } else {
                self.libraries.remove(&app);
            }
            return Err(e);
        }
        Ok(())
    }
    pub fn expand(&self, app: &str, value: &Value) -> Result<Value, Error> {
        self.validate()?;
        budget(value).map_err(|_| Error::Limit)?;
        expand::State::new(self).value(app, value, &[], 0)
    }
    pub fn application(&self, app: &str, key: &str) -> Result<Option<Value>, Error> {
        self.validate()?;
        let Some(value) = self.applications.get(app).and_then(|m| m.get(key)) else {
            return Ok(None);
        };
        // setup:get_env/2 starts with an empty visited set, unlike recursive
        // $env references. The interpreter adds names at that later boundary.
        expand::State::new(self).value(app, value, &[], 0).map(Some)
    }
    fn validate(&self) -> Result<(), Error> {
        let mut bytes =
            self.directories.home.len() + self.directories.data.len() + self.directories.log.len();
        let mut nodes = self.known_names.len();
        for name in &self.known_names {
            bytes = bytes.checked_add(name.len()).ok_or(Error::Limit)?;
        }
        for (app, map) in &self.applications {
            nodes += 1 + map.len();
            bytes += app.len();
            for (key, value) in map {
                let (n, b, _) = budget(value).map_err(|_| Error::Limit)?;
                nodes += n;
                bytes += b + key.len();
            }
        }
        let mut definitions = 0usize;
        for (app, defs) in std::iter::once(("setup", &self.global))
            .chain(self.private.iter().map(|(a, d)| (a.as_str(), d)))
        {
            nodes += 1 + defs.len();
            bytes += app.len();
            definitions += defs.len();
            if definitions > 1024 {
                return Err(Error::Limit);
            }
            for d in defs {
                let v = match &d.value {
                    Variable::Literal(v) | Variable::Expand(v) => v,
                };
                let (n, b, _) = budget(v).map_err(|_| Error::Limit)?;
                nodes += n;
                bytes += b + d.name.len();
            }
        }
        for (a, (l, p)) in &self.libraries {
            nodes += 1;
            bytes += a.len() + l.len() + p.len();
        }
        if bytes > MAX_BYTES || nodes > MAX_NODES || self.known_names.len() > MAX_NODES {
            Err(Error::Limit)
        } else {
            Ok(())
        }
    }
}

pub enum Strategy<'a> {
    User,
    Application { app: &'a str, key: &'a str },
    SchemaDefault,
    Value(&'a Value),
}
pub enum Step<'a> {
    Key(&'a str),
    Select {
        key: &'a str,
        value: &'a Value,
        then: &'a str,
    },
}
pub struct Resolved<'a> {
    user: Value,
    context: &'a Context,
}
impl fmt::Debug for Resolved<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ResolvedConfiguration { .. }")
    }
}
impl Configuration {
    pub fn resolve<'a>(&self, context: &'a Context) -> Result<Resolved<'a>, Error> {
        context.validate()?;
        let user = if expand::expandable(&self.value) {
            context.expand("aeutils", &self.value)?
        } else {
            self.value.clone()
        };
        Ok(Resolved { user, context })
    }
}
impl Resolved<'_> {
    pub fn user_map(&self) -> &Value {
        &self.user
    }
    pub fn get(&self, path: &[&str]) -> Result<Option<&Value>, Error> {
        self.select(&path.iter().map(|s| Step::Key(s)).collect::<Vec<_>>())
    }
    pub fn select(&self, path: &[Step<'_>]) -> Result<Option<&Value>, Error> {
        select(&self.user, path)
    }
    pub fn find(&self, path: &[&str], strategies: &[Strategy<'_>]) -> Result<Option<Value>, Error> {
        for s in strategies {
            let value = match s {
                Strategy::User => self.get(path)?.cloned(),
                Strategy::Application { app, key } => self.context.application(app, key)?,
                Strategy::SchemaDefault => schema_default(path).cloned(),
                Strategy::Value(v) => Some((*v).clone()),
            };
            if value.is_some() {
                return Ok(value);
            }
        }
        Ok(None)
    }
    pub fn user_or_application(
        &self,
        path: &[&str],
        app: &str,
        key: &str,
        fallback: &Value,
    ) -> Result<Value, Error> {
        self.find(
            path,
            &[
                Strategy::User,
                Strategy::Application { app, key },
                Strategy::Value(fallback),
            ],
        )?
        .ok_or(Error::Lookup)
    }
    pub fn config_value(
        &self,
        path: &[&str],
        app: &str,
        key: &str,
        fallback: &Value,
    ) -> Result<Value, Error> {
        self.find(
            path,
            &[
                Strategy::User,
                Strategy::Application { app, key },
                Strategy::SchemaDefault,
                Strategy::Value(fallback),
            ],
        )?
        .ok_or(Error::Lookup)
    }
}
fn select<'a>(value: &'a Value, path: &[Step<'_>]) -> Result<Option<&'a Value>, Error> {
    let Some((step, tail)) = path.split_first() else {
        return Ok(Some(value));
    };
    let next = match (value, step) {
        (Value::Object(m), Step::Key(k)) => m.get(*k),
        (Value::Array(a), _) => {
            let mut found = None;
            for row in a {
                let map = row.as_object().ok_or(Error::Lookup)?;
                match step {
                    Step::Key(k) => {
                        if let Some(v) = map.get(*k) {
                            found = Some(v);
                            break;
                        }
                    }
                    Step::Select { key, value, then } => {
                        if let Some(v) = map.get(*key) {
                            if v != *value {
                                return Err(Error::Lookup);
                            }
                            found = map.get(*then);
                            break;
                        }
                    }
                }
            }
            found
        }
        _ => return Ok(None),
    };
    match next {
        Some(v) if tail.is_empty() => Ok(Some(v)),
        Some(v) if v.is_object() => select(v, tail),
        _ => Ok(None),
    }
}
fn bytes(value: &Value) -> Result<Vec<u8>, Error> {
    text::iolist(value).ok_or(Error::Encoding)
}
fn path(value: &Value) -> Result<PathBuf, Error> {
    let bytes = bytes(value)?;
    if bytes.is_empty() || bytes.contains(&0) {
        return Err(Error::Directory);
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        Ok(std::ffi::OsString::from_vec(bytes).into())
    }
    #[cfg(not(unix))]
    {
        String::from_utf8(bytes)
            .map(PathBuf::from)
            .map_err(|_| Error::Encoding)
    }
}

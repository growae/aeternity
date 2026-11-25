use super::*;
#[derive(Debug, Clone)]
pub struct Engine {
    pub(super) config: Config,
    entries: BTreeMap<Name, Entry>,
    atoms: BTreeSet<String>,
    cells: BTreeMap<Key, Vec<String>>,
}
impl Engine {
    pub fn new(
        config: Config,
        entries: Vec<Entry>,
        atoms: BTreeSet<String>,
    ) -> Result<Self, Error> {
        config.limits.validate()?;
        let mut state = Self {
            config,
            entries: Default::default(),
            atoms,
            cells: Default::default(),
        };
        for entry in entries {
            state.put(entry)?;
        }
        state.check()?;
        Ok(state)
    }
    fn check(&self) -> Result<(), Error> {
        let l = self.config.limits;
        if self.config.rules.len() > l.rules
            || self.entries.len() > l.metrics
            || self.cells.len() > l.cells
            || self.atoms.len() > l.atoms
        {
            return Err(Error::Limit);
        }
        for rule in &self.config.rules {
            l.rule(rule)?;
        }
        if self.retained_bytes() > l.bytes {
            return Err(Error::Limit);
        }
        Ok(())
    }
    pub fn retained_bytes(&self) -> usize {
        self.atoms.iter().map(|a| a.len() + 32).sum::<usize>()
            + self
                .config
                .rules
                .iter()
                .map(|r| 128 + r.name.len() + r.kind.len() + r.datapoints.len() + r.actions.len())
                .sum::<usize>()
            + self
                .entries
                .values()
                .map(|e| 64 + e.kind.len() + e.name.iter().map(Part::bytes).sum::<usize>())
                .sum::<usize>()
            + self
                .cells
                .iter()
                .map(|(k, v)| {
                    64 + k.name.iter().map(Part::bytes).sum::<usize>()
                        + k.datapoint.bytes()
                        + v.iter().map(|a| a.len() + 32).sum::<usize>()
                })
                .sum::<usize>()
    }
    fn transaction<T>(
        &mut self,
        apply: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut next = self.clone();
        let value = apply(&mut next)?;
        next.check()?;
        *self = next;
        Ok(value)
    }
    fn put(&mut self, entry: Entry) -> Result<(), Error> {
        self.config.limits.name(&entry.name)?;
        if entry.kind.chars().count() > 255 {
            return Err(Error::Limit);
        }
        self.atoms.insert(entry.kind.clone());
        self.atoms.extend(entry.name.iter().filter_map(|p| match p {
            Part::Atom(a) => Some(a.clone()),
            _ => None,
        }));
        self.entries.insert(entry.name.clone(), entry);
        Ok(())
    }
    pub fn register(&mut self, entries: Vec<Entry>) -> Result<(), Error> {
        self.transaction(|s| {
            for e in entries {
                s.put(e)?;
            }
            Ok(())
        })
    }
    pub fn unregister(&mut self, name: &Name) -> Result<bool, Error> {
        self.config.limits.name(name)?;
        Ok(self.entries.remove(name).is_some())
    }
    pub fn cells(&self) -> Vec<Cell> {
        self.cells
            .iter()
            .map(|(key, destinations)| Cell {
                key: key.clone(),
                destinations: destinations.clone(),
            })
            .collect()
    }
    pub fn cached(&self, key: &Key) -> Result<Option<Vec<String>>, Error> {
        self.config.limits.name(&key.name)?;
        self.config.limits.name(&vec![key.datapoint.clone()])?;
        Ok(self
            .cells
            .get(key)
            .or_else(|| {
                self.cells.get(&Key {
                    name: key.name.clone(),
                    datapoint: Part::atom("default"),
                })
            })
            .cloned())
    }
    fn populate(&mut self, entries: Vec<Entry>) -> Result<(), Error> {
        for rule in self.config.rules.clone() {
            let (selection, destinations) =
                match rules::apply(&rule, &entries, &mut self.atoms, self.config.limits) {
                    Ok(result) => result,
                    Err(Error::InvalidRule) => continue,
                    Err(error) => return Err(error),
                };
            let Some(points) = selection.datapoints else {
                continue;
            };
            for e in selection.entries {
                for p in &points {
                    self.cells
                        .entry(Key {
                            name: e.name.clone(),
                            datapoint: p.clone(),
                        })
                        .or_insert_with(|| destinations.clone());
                }
            }
            self.check()?;
        }
        Ok(())
    }
    pub(super) fn resolve(&mut self, key: &Key) -> Result<Destinations, Error> {
        if let Some(value) = self.cached(key)? {
            return Ok(Some(value));
        }
        if let Some(entry) = self.entries.get(&key.name).filter(|e| e.enabled).cloned() {
            self.populate(vec![entry])?;
        }
        self.cached(key)
    }
    pub fn destinations(&mut self, key: &Key) -> Result<Destinations, Error> {
        if let Some(value) = self.cached(key)? {
            return Ok(Some(value));
        }
        self.transaction(|s| s.resolve(key))
    }
    pub(super) fn cached_routes(
        &self,
        entries: &[Entry],
        keys: &[Key],
    ) -> Result<Option<Routes>, Error> {
        if entries.iter().any(|e| self.entries.get(&e.name) != Some(e)) {
            return Ok(None);
        }
        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(value) = self.cached(key)? else {
                return Ok(None);
            };
            result.push(Some(value));
        }
        Ok(Some(result))
    }
    pub(super) fn route(&mut self, entries: Vec<Entry>, keys: Vec<Key>) -> Result<Routes, Error> {
        self.transaction(|s| {
            for entry in entries {
                s.put(entry)?;
            }
            for key in &keys {
                s.atoms.extend(
                    key.name
                        .iter()
                        .chain(std::iter::once(&key.datapoint))
                        .filter_map(|p| match p {
                            Part::Atom(a) => Some(a.clone()),
                            _ => None,
                        }),
                );
            }
            s.check()?;
            keys.iter().map(|key| s.resolve(key)).collect()
        })
    }
    pub fn check_config(&mut self, rules: Vec<Rule>) -> Result<(), Error> {
        self.transaction(|s| {
            s.config.rules = rules;
            s.check()?;
            s.populate(s.entries.values().cloned().collect())
        })
    }
    pub fn add(&mut self, key: Key, destination: Destination) -> Result<(), Error> {
        self.cached(&key)?;
        self.transaction(|s| {
            let destinations = s.cells.entry(key).or_default();
            let d = destination.atom();
            if !destinations.contains(&d) {
                destinations.insert(0, d);
            }
            Ok(())
        })
    }
    pub fn delete(&mut self, key: &Key, destination: Destination) -> Result<bool, Error> {
        self.cached(key)?;
        Ok(self.cells.get_mut(key).is_some_and(|v| {
            if let Some(at) = v.iter().position(|a| *a == destination.atom()) {
                v.remove(at);
                true
            } else {
                false
            }
        }))
    }
    pub fn select(
        &mut self,
        command: &[u8],
        candidates: &[Entry],
        filtered: bool,
    ) -> Result<Selection, Error> {
        if candidates.len() > self.config.limits.metrics {
            return Err(Error::Limit);
        }
        for e in candidates {
            self.config.limits.name(&e.name)?;
        }
        self.transaction(|s| {
            let entries = candidates
                .iter()
                .filter(|e| !filtered || (e.enabled && s.entries.contains_key(&e.name)))
                .cloned()
                .collect::<Vec<_>>();
            rules::select(command, &entries, &mut s.atoms, s.config.limits)
        })
    }
}

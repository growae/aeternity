use super::*;
type Group = group::Client<ae_vm_fate::maps::order::Otp26>;

// Only the root owner can publish replacements. Readers copy an exact group
// capability; already accepted work never follows a later source update.
#[derive(Clone)]
pub struct Source(Kind);
#[derive(Clone)]
enum Kind {
    Fixed(Group),
    Managed(Arc<Managed>),
}
struct Managed {
    database: Arc<Database>,
    state: Mutex<State>,
}
struct State {
    highest: u64,
    current: Option<(u64, Group)>,
}
impl From<Group> for Source {
    fn from(group: Group) -> Self {
        Self(Kind::Fixed(group))
    }
}
impl Source {
    pub(crate) fn managed(database: Arc<Database>) -> Self {
        Self(Kind::Managed(Arc::new(Managed {
            database,
            state: Mutex::new(State {
                highest: 0,
                current: None,
            }),
        })))
    }
    pub(crate) fn install(&self, number: u64, group: Group) -> Result<(), Error> {
        let Kind::Managed(m) = &self.0 else {
            return Err(Error::Configuration);
        };
        if !group.uses_database(&m.database) {
            return Err(Error::Configuration);
        }
        let mut s = m.state.lock().unwrap_or_else(|e| e.into_inner());
        if number <= s.highest {
            return Err(Error::Configuration);
        }
        s.highest = number;
        s.current = Some((number, group));
        Ok(())
    }
    pub(crate) fn withdraw(&self) {
        if let Kind::Managed(m) = &self.0 {
            m.state.lock().unwrap_or_else(|e| e.into_inner()).current = None;
        }
    }
    pub fn uses_snapshots(&self, snapshots: &snapshots::Owner) -> bool {
        match &self.0 {
            Kind::Fixed(g) => g.uses_snapshots(snapshots),
            Kind::Managed(m) => snapshots.uses_database(&m.database),
        }
    }
    pub(super) fn current(&self) -> Option<(u64, Group)> {
        match &self.0 {
            Kind::Fixed(g) => Some((0, g.clone())),
            Kind::Managed(m) => m
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .current
                .clone(),
        }
    }
}

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tree {
    Accounts,
    Calls,
    Channels,
    Contracts,
    Names,
    Oracles,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Selection(u8);
impl Default for Selection {
    fn default() -> Self {
        Self(255)
    }
}
impl Selection {
    pub fn new(trees: &[Tree]) -> Result<Self, Error> {
        let mut mask = 0;
        for tree in trees {
            mask |= match tree {
                Tree::Accounts => 1,
                Tree::Calls => 2,
                Tree::Channels => 4,
                Tree::Contracts => 8,
                Tree::Names => 16 | 64,
                Tree::Oracles => 32 | 128,
            };
        }
        if mask == 0 {
            return Err(Error::Corrupt("empty GC selection"));
        }
        Ok(Self(mask))
    }
    pub fn contains(self, index: usize) -> bool {
        index < 8 && self.0 & (1 << index) != 0
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub enabled: bool,
    pub history: u64,
    pub minimum_height: u64,
    pub during_sync: bool,
    pub trees: Selection,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            history: 15_000,
            minimum_height: 0,
            during_sync: false,
            trees: Selection::default(),
        }
    }
}
impl Config {
    pub fn validate(self) -> Result<(), Error> {
        if self.history < 50 {
            return Err(Error::Corrupt("GC history configuration"));
        }
        Ok(())
    }
    pub fn eligible(
        self,
        height: u64,
        key: bool,
        synced: bool,
        state: &State,
    ) -> Result<bool, Error> {
        self.validate()?;
        Ok(self.enabled
            && key
            && (synced || self.during_sync)
            && !state.active()
            && height >= self.minimum_height
            && height
                .checked_sub(state.height)
                .is_some_and(|n| n >= self.history))
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Phase {
    #[default]
    Idle,
    Roots,
    Sweep,
    Complete,
    Cancelled,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct State {
    pub epoch: u64,
    pub height: u64,
    // Each tree retains its last physical boundary when deselected later.
    pub floors: [u64; 8],
    pub phase: Phase,
    pub cursor: Vec<u8>,
    pub records: u64,
    pub nodes: u64,
    pub removed: u64,
    pub bytes: u64,
    pub pending: u64,
}
impl State {
    pub fn active(&self) -> bool {
        matches!(self.phase, Phase::Roots | Phase::Sweep)
    }
    pub fn retains(&self, index: usize, height: u64) -> bool {
        self.floors
            .get(index)
            .is_some_and(|floor| height >= floor.saturating_sub(1))
    }
    pub fn start(&self, height: u64, trees: Selection) -> Result<Self, Error> {
        if self.active() {
            return Err(Error::GcActive);
        }
        if height < self.height {
            return Err(Error::Pruned {
                height,
                floor: self.height,
            });
        }
        let mut floors = self.floors;
        for (i, floor) in floors.iter_mut().enumerate() {
            if trees.contains(i) {
                *floor = height;
            }
        }
        Ok(Self {
            epoch: self.epoch.checked_add(1).ok_or(Error::ResourceLimit)?,
            height,
            floors,
            phase: Phase::Roots,
            ..Default::default()
        })
    }
    pub fn encode(&self) -> Vec<u8> {
        list(vec![
            number(1),
            number(self.epoch),
            number(self.height),
            list(self.floors.iter().map(|n| number(*n)).collect()),
            number(match self.phase {
                Phase::Idle => 0,
                Phase::Roots => 1,
                Phase::Sweep => 2,
                Phase::Complete => 3,
                Phase::Cancelled => 4,
            }),
            binary(&self.cursor),
            number(self.records),
            number(self.nodes),
            number(self.removed),
            number(self.bytes),
            number(self.pending),
        ])
        .encode()
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > 512 {
            return Err(Error::Corrupt("GC record size"));
        }
        let v = decode(bytes)?;
        let f = fields(&v, 11)?;
        if integer(&f[0])? != 1 {
            return Err(Error::Corrupt("GC version"));
        }
        let floors = fields(&f[3], 8)?
            .iter()
            .map(integer)
            .collect::<Result<Vec<_>, _>>()?
            .try_into()
            .map_err(|_| Error::Corrupt("GC floors"))?;
        let state = Self {
            epoch: integer(&f[1])?,
            height: integer(&f[2])?,
            floors,
            phase: match integer(&f[4])? {
                0 => Phase::Idle,
                1 => Phase::Roots,
                2 => Phase::Sweep,
                3 => Phase::Complete,
                4 => Phase::Cancelled,
                _ => return Err(Error::Corrupt("GC phase")),
            },
            cursor: blob(&f[5])?.to_vec(),
            records: integer(&f[6])?,
            nodes: integer(&f[7])?,
            removed: integer(&f[8])?,
            bytes: integer(&f[9])?,
            pending: integer(&f[10])?,
        };
        let valid_cursor = state.cursor.is_empty()
            || match state.phase {
                Phase::Roots => state.cursor.len() == 33 && state.cursor[0] == RECORDS,
                Phase::Sweep => state.cursor.len() == 33 && state.cursor[0] == NODES,
                _ => false,
            };
        if !valid_cursor
            || (!state.active() && state.pending != 0)
            || state.floors.iter().max() != Some(&state.height)
            || state.floors[4] != state.floors[6]
            || state.floors[5] != state.floors[7]
            || (state.epoch == 0) != (state.phase == Phase::Idle)
            || (state.epoch == 0 && state != Self::default())
            || state.encode() != bytes
        {
            return Err(Error::Corrupt("GC state"));
        }
        Ok(state)
    }
}
impl<S: Read> View<S> {
    pub fn retention(&self) -> Result<State, Error> {
        self.source
            .get(GC_STATE)?
            .map(|b| State::decode(&b))
            .transpose()
            .map(Option::unwrap_or_default)
    }
    pub fn require_retained(&self, height: u64) -> Result<(), Error> {
        let floor = self.retention()?.height;
        if height < floor {
            Err(Error::Pruned { height, floor })
        } else {
            Ok(())
        }
    }
    pub(super) fn insertion_trees(
        &self,
        parent: Hash256,
        key_block: bool,
        limits: OverlayLimits,
    ) -> Result<Trees<Overlay<S>, Overlay<S>>, Error> {
        let height = self.require_block(parent)?.header().common().height;
        let floor = self
            .retention()?
            .height
            .saturating_sub(u64::from(key_block));
        if height < floor {
            return Err(Error::Pruned { height, floor });
        }
        self.load_trees(parent, limits)
    }
}

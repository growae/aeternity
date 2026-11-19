// erlang-idna 6cff72747821110169ecfac871b0c69e5064afff.
// Copyright the authors and contributors. MIT: crates/ae-codec/LICENSE-IDNA.
use super::{BadLabel, Error};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetailedError {
    Other(Error),
    Label(LabelError),
}
impl DetailedError {
    pub fn category(&self) -> Error {
        match self {
            Self::Other(e) => e.clone(),
            Self::Label(e) => Error::BadLabel(e.category()),
        }
    }
}
impl From<Error> for DetailedError {
    fn from(e: Error) -> Self {
        Self::Other(e)
    }
}
impl From<LabelError> for DetailedError {
    fn from(e: LabelError) -> Self {
        Self::Label(e)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelError {
    Nfc,
    Hyphen {
        label: Vec<u32>,
        third_fourth: bool,
    },
    InitialCombiner,
    Context {
        label: Vec<u32>,
        codepoint: u32,
        position: usize,
        unassigned: bool,
    },
    Joiner {
        label: Vec<u32>,
        codepoint: u32,
        position: usize,
        kind: JoinerKind,
    },
    Bidi(BidiError),
    Alabel {
        label: Vec<u32>,
        error: Box<DetailedError>,
    },
    TooLong(Vec<u32>),
}
impl LabelError {
    pub fn category(&self) -> BadLabel {
        match self {
            Self::Nfc => BadLabel::Nfc,
            Self::Hyphen { .. } => BadLabel::Hyphen,
            Self::InitialCombiner => BadLabel::InitialCombiner,
            Self::Context { .. } => BadLabel::Context,
            Self::Joiner {
                kind: JoinerKind::J,
                ..
            } => BadLabel::ContextJ,
            Self::Joiner {
                kind: JoinerKind::O,
                ..
            } => BadLabel::ContextO,
            Self::Bidi(_) => BadLabel::Bidi,
            Self::Alabel { .. } => BadLabel::Alabel,
            Self::TooLong(_) => BadLabel::TooLong,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinerKind {
    J,
    O,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BidiError {
    UnknownDirection { label: Vec<u32>, codepoint: u32 },
    FirstDirection(Vec<u32>),
    InvalidRight,
    InvalidLeft,
    MixedNumerals,
    IllegalEnding,
}

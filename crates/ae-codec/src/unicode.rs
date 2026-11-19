// Frozen native Unicode boundaries shared by IDNA and virtual machines.
mod grapheme;
mod grapheme_tables;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Otp26Unicode15,
}
impl Profile {
    // Input is an already-decoded native codepoint list. Callers validate
    // their permitted codepoint domain and bound input traversal separately.
    pub fn first_grapheme_len(self, input: &[u32]) -> usize {
        match self {
            Self::Otp26Unicode15 => grapheme::len(input),
        }
    }
}

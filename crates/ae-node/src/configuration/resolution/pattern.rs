// setup uses PCRE without Unicode mode, operating on individual bytes. The
// scanner maps those bytes to Latin-1 scalars; preserve native character
// classes and the non-consuming final-newline dollar assertion here.
pub(super) fn adapt(name: &str) -> String {
    let mut out = String::new();
    let mut input = name.bytes().peekable();
    let mut class = false;
    while let Some(b) = input.next() {
        match b {
            b'\\' => {
                let Some(c) = input.next() else {
                    out.push('\\');
                    break;
                };
                let replacement = match c {
                    b'h' => Some(r"[\t \x{a0}]"),
                    b'H' => Some(r"[^\t \x{a0}]"),
                    b'v' => Some(r"[\n\x0b\x0c\r\x{85}]"),
                    b'V' => Some(r"[^\n\x0b\x0c\r\x{85}]"),
                    b's' => Some(r"[\t\n\x0b\x0c\r ]"),
                    b'S' => Some(r"[^\t\n\x0b\x0c\r ]"),
                    b'd' => Some("[0-9]"),
                    b'D' => Some("[^0-9]"),
                    b'w' => Some(
                        r"[A-Za-z0-9_\x{aa}\x{b5}\x{ba}\x{c0}-\x{d6}\x{d8}-\x{f6}\x{f8}-\x{ff}]",
                    ),
                    b'W' => Some(
                        r"[^A-Za-z0-9_\x{aa}\x{b5}\x{ba}\x{c0}-\x{d6}\x{d8}-\x{f6}\x{f8}-\x{ff}]",
                    ),
                    b'b' if class => Some(r"\x08"),
                    b'b' => Some(r"\b"),
                    b'B' if !class => Some(r"\B"),
                    _ => None,
                };
                if let Some(s) = replacement {
                    out.push_str(s);
                } else {
                    out.push('\\');
                    out.push(char::from(c));
                }
            }
            b'[' => {
                class = true;
                out.push('[');
            }
            b']' => {
                class = false;
                out.push(']');
            }
            b'$' if !class => out.push_str(r"(?=$|\n\z)"),
            _ => out.push(char::from(b)),
        }
    }
    out
}

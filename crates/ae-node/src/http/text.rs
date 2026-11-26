// The MIT License

// Copyright (c) 2010-2013 Alisdair Sullivan <alisdairsullivan@yahoo.ca>

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Non-strict JSX string cleaning, pinned 3074d4865b3385a050badf7828ad31490d860df5.
// Call this at JSON emission, after parsing/lookup against the original bytes.
// Each input byte produces at most three UTF-8 output bytes.
pub(super) fn binary(mut bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len());
    while !bytes.is_empty() {
        match std::str::from_utf8(bytes) {
            Ok(valid) => {
                output.push_str(valid);
                break;
            }
            Err(error) => {
                let (valid, rest) = bytes.split_at(error.valid_up_to());
                output.push_str(std::str::from_utf8(valid).expect("validated UTF-8 prefix"));
                bytes = rest;
            }
        }
        // JSX consumes the third byte of a surrogate-shaped sequence even
        // when it is not a continuation. Preserve that historical behavior.
        let consumed = if bytes[0] == 0xed && bytes.len() >= 3 && bytes[1] >= 0xa0 {
            3
        } else {
            let continuations = match bytes[0] {
                0xc0..=0xdf => 1,
                0xe0..=0xef => 2,
                0xf0..=0xf7 => 3,
                _ => 0,
            };
            1 + bytes[1..]
                .iter()
                .take(continuations)
                .take_while(|&&byte| (0x80..=0xbf).contains(&byte))
                .count()
        };
        output.push('\u{fffd}');
        bytes = &bytes[consumed..];
    }
    output
}

#[cfg(test)]
mod tests {
    #[test]
    fn original_jsx_binary_text_preserves_native_invalid_sequence_grouping() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/erlang_http_binary_text.json"
        ))
        .unwrap();
        let rows = fixture["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 281);
        for row in rows {
            let input = hex::decode(row["input"].as_str().unwrap()).unwrap();
            let expected = hex::decode(row["normalized"].as_str().unwrap()).unwrap();
            let value = super::binary(&input);
            assert_eq!(value.as_bytes(), expected, "{row}");
            let encoded = hex::decode(row["encoded"].as_str().unwrap()).unwrap();
            let parsed: String = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(value, parsed, "{row}");
            assert!(value.len() <= input.len() * 3);
        }
    }
}

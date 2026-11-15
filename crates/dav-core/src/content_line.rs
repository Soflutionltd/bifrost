//! Shared iCalendar and vCard content-line framing and value encoding.

/// Refuse unwritable controls instead of stripping caller data. An enterprise
/// user expects the saved value to match their input; the account reports the
/// field before issuing a request. TEXT and parameters encode line breaks,
/// while URI and other raw value validation remains protocol-specific.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteError {
    pub field: &'static str,
    pub reason: &'static str,
}

const CONTROL_CHARACTER: &str = "contains a control character";

pub fn text_value(field: &'static str, value: &str) -> Result<String, WriteError> {
    if value
        .chars()
        .any(|ch| ch.is_ascii_control() && !matches!(ch, '\t' | '\r' | '\n'))
    {
        return Err(WriteError {
            field,
            reason: CONTROL_CHARACTER,
        });
    }
    Ok(escape_text(value))
}

pub fn param_value(field: &'static str, value: &str) -> Result<String, WriteError> {
    if value
        .chars()
        .any(|ch| ch.is_ascii_control() && !matches!(ch, '\t' | '\r' | '\n'))
    {
        return Err(WriteError {
            field,
            reason: CONTROL_CHARACTER,
        });
    }
    Ok(escape_param(value))
}

pub fn fold_lines(lines: Vec<String>) -> String {
    let mut folded = String::new();
    for line in lines {
        let mut current = String::new();
        let mut prefix = 0;
        for ch in line.chars() {
            if prefix + current.len() + ch.len_utf8() > 75 {
                folded.push_str(&current);
                folded.push_str("\r\n ");
                current.clear();
                // The folding space occupies one octet of the 75-octet
                // budget on every continuation line.
                prefix = 1;
            }
            current.push(ch);
        }
        folded.push_str(&current);
        folded.push_str("\r\n");
    }
    folded
}

pub fn escape_text(value: &str) -> String {
    // A CRLF pair and a lone CR are each one line break, as `escape_param`
    // reads them. Dropping a lone CR merged the two halves it separated.
    value
        .replace('\\', "\\\\")
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .replace('\n', "\\n")
        .replace(',', "\\,")
        .replace(';', "\\;")
}

pub fn unescape_text(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n' | 'N') => out.push('\n'),
            Some(',') => out.push(','),
            Some(';') => out.push(';'),
            Some('\\') => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

pub fn unescape_param(value: &str) -> String {
    if !value.contains('^') {
        return value.to_string();
    }
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '^' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('\'') => out.push('"'),
            Some('^') => out.push('^'),
            Some(other) => {
                out.push('^');
                out.push(other);
            }
            None => out.push('^'),
        }
    }
    out
}

pub fn escape_param(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '^' => escaped.push_str("^^"),
            '"' => escaped.push_str("^'"),
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                escaped.push_str("^n");
            }
            '\n' => escaped.push_str("^n"),
            _ => escaped.push(ch),
        }
    }
    if escaped.contains([';', ',', ':']) {
        format!("\"{escaped}\"")
    } else {
        escaped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folding_counts_utf8_and_continuation_octets() {
        let line = format!("NOTE:{}", "a\u{e9}".repeat(70));
        let folded = fold_lines(vec![line.clone()]);
        assert!(folded.split("\r\n").all(|line| line.len() <= 75));
        assert_eq!(folded.replace("\r\n ", "").trim_end_matches("\r\n"), line);
    }

    #[test]
    fn escaped_values_preserve_input_and_refuse_unencodable_controls() {
        let value = "backslash\\n,;\u{e9}\nnext";
        assert_eq!(unescape_text(&text_value("note", value).unwrap()), value);
        let parameter = "a\"^\nnext";
        assert_eq!(
            unescape_param(&param_value("name", parameter).unwrap()),
            parameter
        );
        for encode in [text_value, param_value] {
            assert_eq!(encode("name", "a\0b").unwrap_err().field, "name");
        }
    }
}

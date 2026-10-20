use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jiff::civil;
use jiff::tz::Offset;

use crate::compose::Address;

use super::limits::{Defect, MimeLimits};
use super::words::decode_encoded_words;

#[derive(Debug, Clone, Default)]
pub struct HeaderMap {
    entries: Vec<HeaderEntry>,
}

#[derive(Debug, Clone)]
struct HeaderEntry {
    name: String,
    lower: String,
    value: String,
}

impl Address {
    /// Parse an unfolded address-list header value, decoding display names.
    ///
    /// Quoted local parts retain their quotes and quoted-pairs. This tolerant
    /// convenience method returns accepted mailboxes only. Use
    /// `parse_list_checked` before rewriting a draft or submitting recipients.
    pub fn parse_list(value: &str) -> Vec<Self> {
        parse_addresses(value).0
    }

    /// Parse an address list, failing with the original rejected items if any
    /// mailbox cannot be represented. Empty groups and empty lists are valid.
    /// No partial recipient list is returned on failure.
    pub fn parse_list_checked(value: &str) -> Result<Vec<Self>, Vec<String>> {
        let (addresses, rejected) = parse_addresses(value);
        if rejected.is_empty() {
            Ok(addresses)
        } else {
            Err(rejected)
        }
    }
}

impl HeaderMap {
    /// Parse a header block without the separating blank line or message body.
    ///
    /// Repeated fields retain wire order and continuation lines are unfolded.
    /// The caller bounds the block size; this does not apply MIME limits.
    pub fn parse(raw: &[u8]) -> Self {
        parse_header_block(raw)
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|entry| entry.lower.eq_ignore_ascii_case(name))
            .map(|entry| entry.value.as_str())
    }
    pub fn get_all(&self, name: &str) -> impl Iterator<Item = &str> {
        self.entries
            .iter()
            .filter(move |entry| entry.lower.eq_ignore_ascii_case(name))
            .map(|entry| entry.value.as_str())
    }
    pub fn get_decoded(&self, name: &str) -> Option<String> {
        self.get(name)
            .map(|value| decode_encoded_words(value.as_bytes()))
    }
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry.value.as_str()))
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn addresses(&self, name: &str) -> Vec<Address> {
        self.get_all(name).flat_map(Address::parse_list).collect()
    }

    /// Parse every occurrence of a field without silently losing recipients.
    pub fn addresses_checked(&self, name: &str) -> Result<Vec<Address>, Vec<String>> {
        let mut addresses = Vec::new();
        let mut rejected = Vec::new();
        for value in self.get_all(name) {
            let (accepted, failures) = parse_addresses(value);
            addresses.extend(accepted);
            rejected.extend(failures);
        }
        if rejected.is_empty() {
            Ok(addresses)
        } else {
            Err(rejected)
        }
    }

    pub fn date(&self, name: &str) -> Option<SystemTime> {
        self.get(name).and_then(parse_date)
    }
    pub fn message_ids(&self, name: &str) -> Vec<String> {
        self.get_all(name)
            .flat_map(|value| {
                let mut values = Vec::new();
                let mut rest = value;
                while let Some(start) = rest.find('<') {
                    let after = &rest[start + 1..];
                    let Some(end) = after.find('>') else { break };
                    let id = after[..end].trim();
                    if !id.is_empty() {
                        values.push(id.to_owned());
                    }
                    rest = &after[end + 1..];
                }
                values
            })
            .collect()
    }
}

pub(super) fn split_headers(
    raw: &[u8],
    limits: &MimeLimits,
    defects: &mut Vec<Defect>,
) -> (HeaderMap, Vec<u8>) {
    // The separator is looked for across the WHOLE input, not just the first
    // `max_header_bytes`: an oversized header block caps what is PARSED, and
    // must not also throw away the body sitting behind it.
    let (header_bytes, body) = if let Some((header_end, body_start)) = find_separator(raw) {
        (&raw[..header_end], raw[body_start..].to_vec())
    } else {
        defects.push(Defect::MissingHeaderSeparator);
        if looks_like_header(raw) {
            (raw, Vec::new())
        } else {
            (&[][..], raw.to_vec())
        }
    };
    let header_bytes = if header_bytes.len() > limits.max_header_bytes {
        defects.push(Defect::HeaderBlockTooLarge);
        &header_bytes[..limits.max_header_bytes]
    } else {
        header_bytes
    };
    (parse_header_block(header_bytes), body)
}

fn find_separator(raw: &[u8]) -> Option<(usize, usize)> {
    let mut index = 0;
    while index < raw.len() {
        let end = match raw[index..].iter().position(|byte| *byte == b'\n') {
            Some(offset) => index.saturating_add(offset),
            None => raw.len(),
        };
        let line = raw[index..end]
            .strip_suffix(b"\r")
            .unwrap_or(&raw[index..end]);
        let next = if end < raw.len() { end + 1 } else { end };
        if line.is_empty() {
            return Some((index, next));
        }
        index = next;
    }
    None
}

fn looks_like_header(raw: &[u8]) -> bool {
    let line = raw.split(|byte| *byte == b'\n').next().unwrap_or_default();
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    line.iter()
        .position(|byte| *byte == b':')
        .is_some_and(|colon| !line[..colon].iter().any(u8::is_ascii_whitespace))
}

fn parse_header_block(raw: &[u8]) -> HeaderMap {
    let mut map = HeaderMap::default();
    for line in raw.split(|byte| *byte == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line
            .first()
            .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
        {
            if let Some(previous) = map.entries.last_mut() {
                previous.value.push(' ');
                previous
                    .value
                    .push_str(String::from_utf8_lossy(line).trim());
            }
            continue;
        }
        let Some(colon) = line.iter().position(|byte| *byte == b':') else {
            continue;
        };
        let name = String::from_utf8_lossy(&line[..colon]).trim().to_owned();
        if name.is_empty() {
            continue;
        }
        let value = String::from_utf8_lossy(&line[colon + 1..])
            .trim_start()
            .to_owned();
        map.entries.push(HeaderEntry {
            lower: name.to_ascii_lowercase(),
            name,
            value,
        });
    }
    map
}

// Only syntax outside quoted strings, nested comments and angle addresses
// delimits a list or introduces a group. Quoted-pairs escape in comments too.
fn address_syntax(value: &str) -> Vec<(usize, char)> {
    let mut syntax = Vec::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut comments = 0;
    let mut angle = 0;
    let mut literal = false;
    for (index, ch) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' && (quoted || comments > 0) {
            escaped = true;
            continue;
        }
        if comments > 0 {
            match ch {
                '(' => comments += 1,
                ')' => comments -= 1,
                _ => {}
            }
            continue;
        }
        if ch == '"' && !literal {
            quoted = !quoted;
            continue;
        }
        if quoted {
            continue;
        }
        match ch {
            '(' if !literal => comments = 1,
            '[' => literal = true,
            ']' => literal = false,
            '<' if !literal => {
                if angle == 0 {
                    syntax.push((index, ch));
                }
                angle += 1;
            }
            '>' if !literal && angle > 0 => {
                angle -= 1;
                if angle == 0 {
                    syntax.push((index, ch));
                }
            }
            ':' | ',' | ';' if angle == 0 && !literal => syntax.push((index, ch)),
            _ => {}
        }
    }
    syntax
}

fn parse_addresses(value: &str) -> (Vec<Address>, Vec<String>) {
    let mut addresses = Vec::new();
    let mut rejected = Vec::new();
    for original in split_address_items(value) {
        let item = original.trim();
        let syntax = address_syntax(item);
        let item = syntax
            .iter()
            .find(|(_, ch)| *ch == ':')
            .map_or(item, |(index, _)| item[index + 1..].trim());
        if item.is_empty() {
            continue;
        }
        let syntax = address_syntax(item);
        let mailbox = if let Some((open, _)) = syntax.iter().find(|(_, ch)| *ch == '<') {
            syntax
                .iter()
                .find(|(index, ch)| *index > *open && *ch == '>')
                .filter(|(close, _)| item[close + 1..].trim().is_empty())
                .map(|(close, _)| (item[..*open].trim(), item[open + 1..*close].trim()))
        } else {
            Some(("", item))
        };
        let Some((name, address)) = mailbox.filter(|(_, address)| acceptable_addr_spec(address))
        else {
            rejected.push(original.trim().to_owned());
            continue;
        };
        let name = name
            .trim_matches('"')
            .replace("\\\"", "\"")
            .replace("\\\\", "\\");
        addresses.push(Address {
            name: (!name.is_empty()).then(|| decode_encoded_words(name.as_bytes())),
            address: address.to_owned(),
        });
    }
    (addresses, rejected)
}

// This is deliberately not full RFC validation. In particular, do not apply
// an unquoted whitespace rule to a quoted local part (RFC 5322 section 3.2.4).
fn acceptable_addr_spec(value: &str) -> bool {
    let mut quoted = false;
    let mut escaped = false;
    let mut at = None;
    let mut literal = false;
    for (index, ch) in value.char_indices() {
        if escaped {
            if !matches!(ch, ' '..='~' | '\t') {
                return false;
            }
            escaped = false;
            continue;
        }
        match ch {
            '\\' if quoted => escaped = true,
            '"' if at.is_none() => quoted = !quoted,
            '[' if !quoted && at.is_some() => literal = true,
            ']' if literal => literal = false,
            '@' if !quoted && !literal => {
                if at.replace(index).is_some() {
                    return false;
                }
            }
            ' ' | '\t' if quoted => {}
            ch if ch.is_whitespace() || ch.is_control() => return false,
            '<' | '>' | ',' | ';' | ':' | '(' | ')' | '"' if !quoted && !literal => {
                return false;
            }
            _ => {}
        }
    }
    !quoted && !escaped && !literal && at.is_some_and(|index| index > 0 && index + 1 < value.len())
}

fn split_address_items(value: &str) -> Vec<&str> {
    let mut items = Vec::new();
    let mut start = 0;
    for (index, ch) in address_syntax(value) {
        if matches!(ch, ',' | ';') {
            items.push(&value[start..index]);
            start = index + 1;
        }
    }
    items.push(&value[start..]);
    items
}

fn parse_date(value: &str) -> Option<SystemTime> {
    let value = value.split_once(',').map_or(value, |(_, rest)| rest).trim();
    let mut words = value.split_whitespace().peekable();
    // The obsolete `day-of-week` is optional AND sometimes arrives without the
    // comma this function split on, so a leading non-numeric word is dropped.
    if words
        .peek()
        .is_some_and(|word| word.parse::<u32>().is_err())
    {
        words.next();
    }
    let day: i8 = words.next()?.parse().ok()?;
    let month: i8 = match words.next()?.to_ascii_lowercase().as_str() {
        "jan" => 1,
        "feb" => 2,
        "mar" => 3,
        "apr" => 4,
        "may" => 5,
        "jun" => 6,
        "jul" => 7,
        "aug" => 8,
        "sep" => 9,
        "oct" => 10,
        "nov" => 11,
        "dec" => 12,
        _ => return None,
    };
    let raw_year: i16 = words.next()?.parse().ok()?;
    let year = if raw_year < 50 {
        raw_year + 2000
    } else if raw_year < 100 {
        raw_year + 1900
    } else {
        raw_year
    };
    let time = words.next()?;
    let zone = words.next().unwrap_or("-0000");
    let time = civil::Time::strptime("%H:%M:%S", time)
        .or_else(|_| civil::Time::strptime("%H:%M", time))
        .ok()?;
    let offset = parse_zone(zone)?;
    let local = civil::Date::new(year, month, day).ok()?.to_datetime(time);
    // RFC 5322 zones are fixed offsets, so the local time is unambiguous:
    // there is no DST gap or fold to disambiguate.
    let timestamp = Offset::from_seconds(offset)
        .ok()?
        .to_timestamp(local)
        .ok()?
        .as_second();
    if timestamp >= 0 {
        UNIX_EPOCH.checked_add(Duration::from_secs(timestamp.unsigned_abs()))
    } else {
        UNIX_EPOCH.checked_sub(Duration::from_secs(timestamp.unsigned_abs()))
    }
}

fn parse_zone(zone: &str) -> Option<i32> {
    let named = match zone.to_ascii_uppercase().as_str() {
        "UT" | "GMT" => Some(0),
        "EST" => Some(-5 * 3600),
        "EDT" => Some(-4 * 3600),
        "CST" => Some(-6 * 3600),
        "CDT" => Some(-5 * 3600),
        "MST" => Some(-7 * 3600),
        "MDT" => Some(-6 * 3600),
        "PST" => Some(-8 * 3600),
        "PDT" => Some(-7 * 3600),
        value if value.len() == 1 && value.as_bytes()[0].is_ascii_alphabetic() => Some(0),
        _ => None,
    };
    if let Some(offset) = named {
        return Some(offset);
    }
    let bytes = zone.as_bytes();
    if bytes.len() != 5 || !matches!(bytes[0], b'+' | b'-') {
        return None;
    }
    let hours: i32 = zone[1..3].parse().ok()?;
    let minutes: i32 = zone[3..5].parse().ok()?;
    (hours <= 23 && minutes <= 59)
        .then_some((hours * 3600 + minutes * 60) * if bytes[0] == b'+' { 1 } else { -1 })
}

#[cfg(test)]
#[path = "header_tests.rs"]
mod tests;

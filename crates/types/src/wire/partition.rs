//! Stable inventory checkpoint key codec. Completion markers belong to the engine.

use crate::Partition;

/// Durable inventory partition key. Unknown rows retain their original bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum InventoryKey {
    Full,
    Time {
        from: Option<i64>,
        to: Option<i64>,
    },
    Uid {
        uid_validity: u32,
        from: u64,
        to: u64,
    },
    Page {
        from: u32,
        to: u32,
    },
    Opaque(Partition),
}

impl InventoryKey {
    #[must_use]
    pub fn decode(partition: &Partition) -> Self {
        let Ok(text) = std::str::from_utf8(&partition.0) else {
            return Self::Opaque(partition.clone());
        };
        if text == "full" {
            return Self::Full;
        }
        if let Some(rest) = text.strip_prefix("uid:") {
            let parts: Vec<_> = rest.split(':').collect();
            if let [epoch, from, to] = parts.as_slice()
                && let (Ok(uid_validity), Ok(from), Ok(to)) =
                    (epoch.parse(), from.parse(), to.parse())
                && crate::UidWindow::new(uid_validity, from, to).is_some()
            {
                return Self::Uid {
                    uid_validity,
                    from,
                    to,
                };
            }
            // Legacy keys carry no epoch and cannot safely resume a UID window.
            return Self::Opaque(partition.clone());
        }
        if let Some((kind, rest)) = text.split_once(':')
            && let Some((from, to)) = rest.split_once(':')
        {
            match kind {
                "page" => {
                    if let (Ok(from), Ok(to)) = (from.parse(), to.parse())
                        && crate::PageWindow::new(from, to).is_some()
                    {
                        return Self::Page { from, to };
                    }
                }
                "time" => {
                    let parse = |s: &str| {
                        if s.is_empty() {
                            Some(None)
                        } else {
                            s.parse::<i64>().ok().map(Some)
                        }
                    };
                    if let (Some(from), Some(to)) = (parse(from), parse(to))
                        && crate::TimeWindow::new(from, to).is_some()
                    {
                        return Self::Time { from, to };
                    }
                }
                _ => {}
            }
        }
        Self::Opaque(partition.clone())
    }

    #[must_use]
    pub fn encode(&self) -> Partition {
        let text = match self {
            Self::Full => "full".to_owned(),
            Self::Time { from, to } => {
                format!("time:{}:{}", optional_i64(*from), optional_i64(*to))
            }
            Self::Uid {
                uid_validity,
                from,
                to,
            } => format!("uid:{uid_validity}:{from}:{to}"),
            Self::Page { from, to } => format!("page:{from}:{to}"),
            Self::Opaque(partition) => return partition.clone(),
        };
        Partition(text.into_bytes())
    }
}

fn optional_i64(value: Option<i64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

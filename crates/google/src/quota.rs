#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApiSurface {
    Gmail,
    Calendar,
    People,
    Drive,
}

pub(crate) use crate::gmail::quota::*;

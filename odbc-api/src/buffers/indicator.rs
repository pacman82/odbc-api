use std::convert::TryInto;

use odbc_sys::{NO_TOTAL, NULL_DATA};

/// Indicates existence and length of a value.
#[derive(Debug, PartialEq, Eq)]
pub enum Indicator {
    /// Field does not exist
    Null,
    /// Field exists, but its length had not be reported by the driver.
    NoTotal,
    /// Fields exists. Value indicates number of bytes required to store the value. In case of
    /// truncated data, this is the untruncated true length.
    Length(usize),
}

impl Indicator {
    /// Creates an indicator from an `isize` indicator value returned by ODBC. Users of this crate
    /// have likely no need to call this method.
    pub fn from_isize(indicator: isize) -> Self {
        match indicator {
            NULL_DATA => Indicator::Null,
            NO_TOTAL => Indicator::NoTotal,
            other => Indicator::Length(
                other
                    .try_into()
                    .expect("Length indicator must be non-negative."),
            ),
        }
    }
}

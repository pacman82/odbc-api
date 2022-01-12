use odbc_sys::{NO_TOTAL, NULL_DATA};

/// Indicates existence and length of a value.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Indicator {
    /// Field does not exist
    Null,
    /// Field exists, but its length had not be reported by the driver.
    NoTotal,
    /// Fields exists. Value indicates number of bytes required to store the value. In case of
    /// truncated data, this is the true length of the data, before truncation occurred.
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

    /// Creates an indicator value as required by the ODBC C API.
    pub fn to_isize(self) -> isize {
        match self {
            Indicator::Null => NULL_DATA,
            Indicator::NoTotal => NO_TOTAL,
            Indicator::Length(len) => len.try_into().unwrap(),
        }
    }
}

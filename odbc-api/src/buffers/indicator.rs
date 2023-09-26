use std::fmt::Display;

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

impl Display for Indicator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Indicator::Null => write!(f, "Null"),
            Indicator::NoTotal => write!(f, "No Total"),
            Indicator::Length(length) => write!(f, "{length}"),
        }
    }
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

    /// Does this indicator imply truncation for a value of the given length?
    ///
    /// `length_in_buffer` is specified in bytes without terminating zeroes.
    pub fn is_truncated(self, length_in_buffer: usize) -> bool {
        match self {
            Indicator::Null => false,
            Indicator::NoTotal => true,
            Indicator::Length(complete_length) => complete_length > length_in_buffer,
        }
    }

    /// Only `true` if the indicator is the equivalent to [`odbc_sys::NULL_DATA`], indicating a
    /// non-existing value.
    pub fn is_null(self) -> bool {
        match self {
            Indicator::Null => true,
            Indicator::NoTotal | Indicator::Length(_) => false,
        }
    }

    /// If the indicator is [`Indicator::Length`] this is [`Some`].
    pub fn value_len(self) -> Option<usize> {
        if let Indicator::Length(len) = self {
            Some(len)
        } else {
            None
        }
    }
}

/// Additional information in case of writing a value into too short a buffer.
pub struct TruncationDiagnostics {
    /// Size indicator reported by the driver indicating the size of the complete value in the DBMS.
    pub indicator: Indicator,
}

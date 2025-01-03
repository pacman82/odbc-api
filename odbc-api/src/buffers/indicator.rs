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
            other => Indicator::Length(other.try_into().expect(
                "Length indicator must be non-negative. This is not necessarily a programming \
                error, in the application. If you are on a 64Bit platfrom and the isize value has \
                been returned by the driver there may be a better exlpanation for what went wrong: \
                In the past some driver managers and drivers assumed SQLLEN to be 32Bits even on \
                64Bit platforms. Please ask your vendor for a version of the driver which is \
                correctly build using 64Bits for SQLLEN.",
            )),
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
    pub fn length(self) -> Option<usize> {
        if let Indicator::Length(len) = self {
            Some(len)
        } else {
            None
        }
    }
}

use odbc_sys::{SmallInt, SqlDataType, ULen};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Enumeration over valid SQL Data Types supported by ODBC
pub enum DataType {
    /// The type is not known.
    Unknown,
    /// `Char(n)`. Character string of fixed length.
    Char {
        /// Column size in characters (excluding terminating zero).
        length: ULen,
    },
    /// `Numeric(p,s). Signed, exact, numeric value with a precision p and scale s (1 <= p <= 15; s
    /// <= p)
    Numeric {
        /// Total number of digits.
        precision: ULen,
        /// Number of decimal digits.
        scale: SmallInt,
    },
    /// `Decimal(p,s)`. Signed, exact, numeric value with a precision of at least p and scale s. The
    /// maximum precision is driver-defined. (1 <= p <= 15; s <= p)
    Decimal {
        /// Total number of digits.
        precision: ULen,
        /// Number of decimal digits.
        scale: SmallInt,
    },
    /// `Integer`. 32 Bit Integer
    Integer,
    /// `Smallint`. 16 Bit Integer
    SmallInt,
    /// `Float(p)`. Signed, approximate, numeric value with a binary precision of at least p. The
    /// maximum precision is driver-defined.
    Float,
    /// `Real`. Signed, approximate, numeric value with a binary precision 24 (zero or absolute
    /// value 10[-38] to 10[38]).
    Real,
    /// `Double Precision`. Signed, approximate, numeric value with a binary precision 53 (zero or
    /// absolute value 10[-308] to 10[308]).
    Double,
    /// `Varchar(n)`. Variable length character string.
    Varchar {
        /// Maximum length of the character string (excluding terminating zero).
        length: ULen,
    },
    /// `Date`. Year, month, and day fields, conforming to the rules of the Gregorian calendar.
    Date,
    /// `Time`. Hour, minute, and second fields, with valid values for hours of 00 to 23, valid
    /// values for minutes of 00 to 59, and valid values for seconds of 00 to 61. Precision p
    /// indicates the seconds precision.
    Time { precision: SmallInt },
    /// `Timestamp`. Year, month, day, hour, minute, and second fields, with valid values as defined
    /// for the Date and Time variants.
    Timestamp { precision: SmallInt },
    /// The driver returned a type, but it is not among the other types of these enumeration. This
    /// is a catchall, in case the library is incomplete, or the data source supports custom or
    /// non-standard types.
    Other {
        /// Type of the column
        data_type: SqlDataType,
        /// Size of column element
        column_size: ULen,
        decimal_digits: SmallInt,
    },
}

impl DataType {
    pub fn new(data_type: SqlDataType, column_size: ULen, decimal_digits: SmallInt) -> Self {
        match data_type {
            SqlDataType::UNKNOWN_TYPE => DataType::Unknown,
            SqlDataType::CHAR => DataType::Char {
                length: column_size,
            },
            SqlDataType::NUMERIC => DataType::Numeric {
                precision: column_size,
                scale: decimal_digits,
            },
            SqlDataType::DECIMAL => DataType::Decimal {
                precision: column_size,
                scale: decimal_digits,
            },
            SqlDataType::INTEGER => DataType::Integer,
            SqlDataType::SMALLINT => DataType::SmallInt,
            SqlDataType::FLOAT => DataType::Float,
            SqlDataType::REAL => DataType::Real,
            SqlDataType::DOUBLE => DataType::Double,
            SqlDataType::VARCHAR => DataType::Varchar {
                length: column_size,
            },
            SqlDataType::DATE => DataType::Date,
            SqlDataType::TIME => DataType::Time {
                precision: decimal_digits,
            },
            SqlDataType::TIMESTAMP => DataType::Timestamp {
                precision: decimal_digits,
            },
            other => DataType::Other {
                data_type: other,
                column_size,
                decimal_digits,
            },
        }
    }
}

impl Default for DataType {
    fn default() -> Self {
        DataType::Unknown
    }
}

use odbc_sys::{SqlDataType, ULen};

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
        scale: i16,
    },
    /// `Decimal(p,s)`. Signed, exact, numeric value with a precision of at least p and scale s. The
    /// maximum precision is driver-defined. (1 <= p <= 15; s <= p)
    Decimal {
        /// Total number of digits.
        precision: ULen,
        /// Number of decimal digits.
        scale: i16,
    },
    /// `Integer`. 32 Bit Integer
    Integer,
    /// `Smallint`. 16 Bit Integer
    SmallInt,
    /// `Float(p)`. Signed, approximate, numeric value with a binary precision of at least p. The
    /// maximum precision is driver-defined.
    ///
    /// Depending on the implementation binary precision is either 24 (`f32`) or 53 (`f64`).
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
    Time { precision: i16 },
    /// `Timestamp`. Year, month, day, hour, minute, and second fields, with valid values as defined
    /// for the Date and Time variants.
    Timestamp { precision: i16 },
    /// `BIGINT`. Exact numeric value with precision 19 (if signed) or 20 (if unsigned) and scale 0
    /// (signed: -2[63] <= n <= 2[63] - 1, unsigned: 0 <= n <= 2[64] - 1). Has no corresponding type
    /// in SQL-92.
    Bigint,
    /// `TINYINT`. Exact numeric value with precision 3 and scale 0 (signed: -128 <= n <= 127,
    /// unsigned: 0 <= n <= 255)
    Tinyint,
    /// `BIT`. Single bit binary data.
    Bit,
    /// The driver returned a type, but it is not among the other types of these enumeration. This
    /// is a catchall, in case the library is incomplete, or the data source supports custom or
    /// non-standard types.
    Other {
        /// Type of the column
        data_type: SqlDataType,
        /// Size of column element
        column_size: ULen,
        decimal_digits: i16,
    },
}

impl DataType {
    pub fn new(data_type: SqlDataType, column_size: ULen, decimal_digits: i16) -> Self {
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
            SqlDataType::EXT_BIG_INT => DataType::Bigint,
            SqlDataType::EXT_TINY_INT => DataType::Tinyint,
            SqlDataType::EXT_BIT => DataType::Bit,
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

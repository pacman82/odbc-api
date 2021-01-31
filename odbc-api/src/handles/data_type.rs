use odbc_sys::SqlDataType;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Enumeration over valid SQL Data Types supported by ODBC
pub enum DataType {
    /// The type is not known.
    Unknown,
    /// `Char(n)`. Character string of fixed length.
    Char {
        /// Column size in characters (excluding terminating zero).
        length: usize,
    },
    /// `Numeric(p,s). Signed, exact, numeric value with a precision p and scale s (1 <= p <= 15; s
    /// <= p)
    Numeric {
        /// Total number of digits.
        precision: usize,
        /// Number of decimal digits.
        scale: i16,
    },
    /// `Decimal(p,s)`. Signed, exact, numeric value with a precision of at least p and scale s.
    /// The maximum precision is driver-defined. (1 <= p <= 15; s <= p)
    Decimal {
        /// Total number of digits.
        precision: usize,
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
    /// value 10^-38] to 10^38).
    Real,
    /// `Double Precision`. Signed, approximate, numeric value with a binary precision 53 (zero or
    /// absolute value 10^-308 to 10^308).
    Double,
    /// `Varchar(n)`. Variable length character string.
    Varchar {
        /// Maximum length of the character string (excluding terminating zero).
        length: usize,
    },
    /// `NVARCHAR(n)`. Variable length character string. Indicates the use of wide character strings
    /// and use of UCS2 encoding on the side of the database.
    WVarchar {
        /// Maximum length of the character string (excluding terminating zero).
        length: usize,
    },
    /// `Date`. Year, month, and day fields, conforming to the rules of the Gregorian calendar.
    Date,
    /// `Time`. Hour, minute, and second fields, with valid values for hours of 00 to 23, valid
    /// values for minutes of 00 to 59, and valid values for seconds of 00 to 61. Precision p
    /// indicates the seconds precision.
    Time {
        /// Number of radix ten digits used to represent the timestamp after the decimal points.
        /// E.g. Milliseconds would be represented by precision 3, Micorseconds by 6 and Nanoseconds
        /// by 9.
        precision: i16,
    },
    /// `Timestamp`. Year, month, day, hour, minute, and second fields, with valid values as
    /// defined for the Date and Time variants.
    Timestamp {
        /// Number of radix ten digits used to represent the timestamp after the decimal points.
        /// E.g. Milliseconds would be represented by precision 3, Micorseconds by 6 and Nanoseconds
        /// by 9.
        precision: i16,
    },
    /// `BIGINT`. Exact numeric value with precision 19 (if signed) or 20 (if unsigned) and scale 0
    /// (signed: -2^63 <= n <= 2^63 - 1, unsigned: 0 <= n <= 2^64 - 1). Has no corresponding
    /// type in SQL-92.
    BigInt,
    /// `TINYINT`. Exact numeric value with precision 3 and scale 0 (signed: -128 <= n <= 127,
    /// unsigned: 0 <= n <= 255)
    TinyInt,
    /// `BIT`. Single bit binary data.
    Bit,
    /// `VARBINARY`. Variable sized type for binary data.
    Varbinary { length: usize },
    /// The driver returned a type, but it is not among the other types of these enumeration. This
    /// is a catchall, in case the library is incomplete, or the data source supports custom or
    /// non-standard types.
    Other {
        /// Type of the column
        data_type: SqlDataType,
        /// Size of column element
        column_size: usize,
        /// Decimal digits returned for the column element. Exact meaning if any depends on the
        /// `data_type` field.
        decimal_digits: i16,
    },
}

impl DataType {
    /// This constructor is useful to create an instance of the enumeration using values returned by
    /// ODBC Api calls like `SQLDescribeCol`, rather than just initializing a variant directly.
    pub fn new(data_type: SqlDataType, column_size: usize, decimal_digits: i16) -> Self {
        match data_type {
            SqlDataType::UNKNOWN_TYPE => DataType::Unknown,
            SqlDataType::CHAR => DataType::Char {
                length: column_size,
            },
            SqlDataType::VARCHAR => DataType::Varchar {
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
            SqlDataType::DATE => DataType::Date,
            SqlDataType::TIME => DataType::Time {
                precision: decimal_digits,
            },
            SqlDataType::TIMESTAMP => DataType::Timestamp {
                precision: decimal_digits,
            },
            SqlDataType::EXT_BIG_INT => DataType::BigInt,
            SqlDataType::EXT_TINY_INT => DataType::TinyInt,
            SqlDataType::EXT_BIT => DataType::Bit,
            SqlDataType::EXT_W_VARCHAR => DataType::WVarchar {
                length: column_size,
            },
            other => DataType::Other {
                data_type: other,
                column_size,
                decimal_digits,
            },
        }
    }

    /// The associated `data_type` discriminator for this variant.
    pub fn data_type(&self) -> SqlDataType {
        match self {
            DataType::Unknown => SqlDataType::UNKNOWN_TYPE,
            DataType::Varbinary { .. } => SqlDataType::EXT_BINARY,
            DataType::Char { .. } => SqlDataType::CHAR,
            DataType::Numeric { .. } => SqlDataType::NUMERIC,
            DataType::Decimal { .. } => SqlDataType::DECIMAL,
            DataType::Integer => SqlDataType::INTEGER,
            DataType::SmallInt => SqlDataType::SMALLINT,
            DataType::Float => SqlDataType::FLOAT,
            DataType::Real => SqlDataType::REAL,
            DataType::Double => SqlDataType::DOUBLE,
            DataType::Varchar { .. } => SqlDataType::VARCHAR,
            DataType::Date => SqlDataType::DATE,
            DataType::Time { .. } => SqlDataType::TIME,
            DataType::Timestamp { .. } => SqlDataType::TIMESTAMP,
            DataType::BigInt => SqlDataType::EXT_BIG_INT,
            DataType::TinyInt => SqlDataType::EXT_TINY_INT,
            DataType::Bit => SqlDataType::EXT_BIT,
            DataType::WVarchar { .. } => SqlDataType::EXT_W_VARCHAR,
            DataType::Other { data_type, .. } => *data_type,
        }
    }

    /// Return the column size, as it is required to bind the data type as a parameter. This implies
    // it can be zero for fixed sized types. See also [crates::Cursor::describe_col].
    pub fn column_size(&self) -> usize {
        match self {
            DataType::Unknown
            | DataType::Integer
            | DataType::SmallInt
            | DataType::Float
            | DataType::Real
            | DataType::Double
            | DataType::Date
            | DataType::Time { .. }
            | DataType::Timestamp { .. }
            | DataType::BigInt
            | DataType::TinyInt
            | DataType::Bit => 0,
            DataType::Char { length }
            | DataType::Varchar { length }
            | DataType::Varbinary { length }
            | DataType::WVarchar { length } => *length,
            DataType::Numeric { precision, .. } | DataType::Decimal { precision, .. } => *precision,
            DataType::Other { column_size, .. } => *column_size,
        }
    }

    /// Return the number of decimal digits as required to bind the data type as a parameter.
    pub fn decimal_digits(&self) -> i16 {
        match self {
            DataType::Unknown
            | DataType::Char { .. }
            | DataType::Integer
            | DataType::SmallInt
            | DataType::Float
            | DataType::Real
            | DataType::Double
            | DataType::Varchar { .. }
            | DataType::WVarchar { .. }
            | DataType::Varbinary { .. }
            | DataType::Date
            | DataType::BigInt
            | DataType::TinyInt
            | DataType::Bit => 0,
            DataType::Numeric { scale, .. } | DataType::Decimal { scale, .. } => *scale,
            DataType::Time { precision } | DataType::Timestamp { precision } => *precision,
            DataType::Other { decimal_digits, .. } => *decimal_digits,
        }
    }

    /// The maximum number of characters needed to display data in character form.
    ///
    /// See: <https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/display-size>
    pub fn display_size(&self) -> Option<usize> {
        match self {
            DataType::Unknown
            | DataType::Other {
                data_type: _,
                column_size: _,
                decimal_digits: _,
            } => None,
            // Each binary byte is represented by a 2-digit hexadecimal number.
            DataType::Varbinary { length } => Some(*length * 2),
            // The defined (for fixed types) or maximum (for variable types) number of characters
            // needed to display the data in character form.
            DataType::Varchar { length }
            | DataType::WVarchar { length }
            | DataType::Char { length } => Some(*length),
            // The precision of the column plus 2 (a sign, precision digits, and a decimal point).
            // For example, the display size of a column defined as NUMERIC(10,3) is 12.
            DataType::Numeric {
                precision,
                scale: _,
            }
            | DataType::Decimal {
                precision,
                scale: _,
            } => Some(precision + 2),
            // 11 if signed (a sign and 10 digits) or 10 if unsigned (10 digits).
            DataType::Integer => Some(11),
            // 6 if signed (a sign and 5 digits) or 5 if unsigned (5 digits).
            DataType::SmallInt => Some(6),
            // 24 (a sign, 15 digits, a decimal point, the letter E, a sign, and 3 digits).
            DataType::Float | DataType::Double => Some(24),
            // 14 (a sign, 7 digits, a decimal point, the letter E, a sign, and 2 digits).
            DataType::Real => Some(14),
            // 10 (a date in the format yyyy-mm-dd).
            DataType::Date => Some(10),
            // 8 (a time in the format hh:mm:ss)
            // or
            // 9 + s (a time in the format hh:mm:ss[.fff...], where s is the fractional seconds
            // precision).
            DataType::Time { precision } => Some(if *precision == 0 {
                8
            } else {
                9 + *precision as usize
            }),
            // 19 (for a timestamp in the yyyy-mm-dd hh:mm:ss format)
            // or
            // 20 + s (for a timestamp in the yyyy-mm-dd hh:mm:ss[.fff...] format, where s is the
            // fractional seconds precision).
            DataType::Timestamp { precision } => Some(if *precision == 0 {
                19
            } else {
                20 + *precision as usize
            }),
            // 20 (a sign and 19 digits if signed or 20 digits if unsigned).
            DataType::BigInt => Some(20),
            // 4 if signed (a sign and 3 digits) or 3 if unsigned (3 digits).
            DataType::TinyInt => Some(4),
            // 1 digit.
            DataType::Bit => Some(1),
        }
    }

    /// The (maximum) length of the utf-8 representation in bytes.
    pub fn utf8_len(&self) -> Option<usize> {
        match self {
            // One character may need up to four bytes to be represented in utf-8.
            DataType::Varchar { length }
            | DataType::WVarchar { length }
            | DataType::Char { length } => Some(length * 4),
            other => other.display_size(),
        }
    }
}

impl Default for DataType {
    fn default() -> Self {
        DataType::Unknown
    }
}

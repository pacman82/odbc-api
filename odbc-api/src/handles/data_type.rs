use std::num::NonZeroUsize;

use odbc_sys::SqlDataType;

/// The relational type of the column. Think of it as the type used in the `CREATE TABLE` statement
/// then creating the database.
///
/// There might be a mismatch between the types supported by your database and the types defined in
/// ODBC. E.g. ODBC does not have a timestamp with timezone type, theras Postgersql and Microsoft
/// SQL Server both have one. In such cases it is up to the specific ODBC driver what happens.
/// Microsoft SQL Server return a custom type, with its meaning specific to that driver. PostgreSQL
/// identifies that column as an ordinary ODBC timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
/// Enumeration over valid SQL Data Types supported by ODBC
pub enum DataType {
    /// The type is not known.
    #[default]
    Unknown,
    /// `Char(n)`. Character string of fixed length.
    Char {
        /// Column size in characters (excluding terminating zero).
        length: Option<NonZeroUsize>,
    },
    /// `NChar(n)`. Character string of fixed length.
    WChar {
        /// Column size in characters (excluding terminating zero).
        length: Option<NonZeroUsize>,
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
    Float { precision: usize },
    /// `Real`. Signed, approximate, numeric value with a binary precision 24 (zero or absolute
    /// value 10^-38] to 10^38).
    Real,
    /// `Double Precision`. Signed, approximate, numeric value with a binary precision 53 (zero or
    /// absolute value 10^-308 to 10^308).
    Double,
    /// `Varchar(n)`. Variable length character string.
    Varchar {
        /// Maximum length of the character string (excluding terminating zero). Whether this length
        /// is to be interpreted as bytes or Codepoints is ambigious and depends on the datasource.
        ///
        /// E.g. For Microsoft SQL Server this is the binary length, theras for a MariaDB this
        /// refers to codepoints in case of UTF-8 encoding. If you need the binary size query the
        /// octet length for that column instead.
        ///
        /// To find out how to interpret this value for a particular datasource you can use the
        /// `odbcsv` command line tool `list-columns` subcommand and query a Varchar column. If the
        /// buffer/octet length matches the column size, you can interpret this as the byte length.
        length: Option<NonZeroUsize>,
    },
    /// `NVARCHAR(n)`. Variable length character string. Indicates the use of wide character strings
    /// and use of UCS2 encoding on the side of the database.
    WVarchar {
        /// Maximum length of the character string (excluding terminating zero).
        length: Option<NonZeroUsize>,
    },
    /// `TEXT`. Variable length characeter string for long text objects.
    LongVarchar {
        /// Maximum length of the character string (excluding terminating zero). Maximum size
        /// depends on the capabilities of the driver and datasource. E.g. its 2^31 - 1 for MSSQL.
        length: Option<NonZeroUsize>,
    },
    /// `BLOB`. Variable length data for long binary objects.
    LongVarbinary {
        /// Maximum length of the binary data. Maximum size depends on the capabilities of the
        /// driver and datasource.
        length: Option<NonZeroUsize>,
    },
    /// `Date`. Year, month, and day fields, conforming to the rules of the Gregorian calendar.
    Date,
    /// `Time`. Hour, minute, and second fields, with valid values for hours of 00 to 23, valid
    /// values for minutes of 00 to 59, and valid values for seconds of 00 to 61. Precision p
    /// indicates the seconds precision.
    Time {
        /// Number of radix ten digits used to represent the timestamp after the decimal points.
        /// E.g. Milliseconds would be represented by precision 3, Microseconds by 6 and Nanoseconds
        /// by 9.
        precision: i16,
    },
    /// `Timestamp`. Year, month, day, hour, minute, and second fields, with valid values as
    /// defined for the Date and Time variants.
    Timestamp {
        /// Number of radix ten digits used to represent the timestamp after the decimal points.
        /// E.g. Milliseconds would be represented by precision 3, Microseconds by 6 and Nanoseconds
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
    /// `VARBINARY(n)`. Type for variable sized binary data.
    Varbinary { length: Option<NonZeroUsize> },
    /// `BINARY(n)`. Type for fixed sized binary data.
    Binary { length: Option<NonZeroUsize> },
    /// The driver returned a type, but it is not among the other types of these enumeration. This
    /// is a catchall, in case the library is incomplete, or the data source supports custom or
    /// non-standard types.
    Other {
        /// Type of the column
        data_type: SqlDataType,
        /// Size of column element
        column_size: Option<NonZeroUsize>,
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
            SqlDataType::EXT_LONG_VARCHAR => DataType::LongVarchar {
                length: NonZeroUsize::new(column_size),
            },
            SqlDataType::EXT_BINARY => DataType::Binary {
                length: NonZeroUsize::new(column_size),
            },
            SqlDataType::EXT_VAR_BINARY => DataType::Varbinary {
                length: NonZeroUsize::new(column_size),
            },
            SqlDataType::EXT_LONG_VAR_BINARY => DataType::LongVarbinary {
                length: NonZeroUsize::new(column_size),
            },
            SqlDataType::CHAR => DataType::Char {
                length: NonZeroUsize::new(column_size),
            },
            SqlDataType::VARCHAR => DataType::Varchar {
                length: NonZeroUsize::new(column_size),
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
            SqlDataType::FLOAT => DataType::Float {
                precision: column_size,
            },
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
                length: NonZeroUsize::new(column_size),
            },
            SqlDataType::EXT_W_CHAR => DataType::WChar {
                length: NonZeroUsize::new(column_size),
            },
            other => DataType::Other {
                data_type: other,
                column_size: NonZeroUsize::new(column_size),
                decimal_digits,
            },
        }
    }

    /// The associated `data_type` discriminator for this variant.
    pub fn data_type(&self) -> SqlDataType {
        match self {
            DataType::Unknown => SqlDataType::UNKNOWN_TYPE,
            DataType::Binary { .. } => SqlDataType::EXT_BINARY,
            DataType::Varbinary { .. } => SqlDataType::EXT_VAR_BINARY,
            DataType::LongVarbinary { .. } => SqlDataType::EXT_LONG_VAR_BINARY,
            DataType::Char { .. } => SqlDataType::CHAR,
            DataType::Numeric { .. } => SqlDataType::NUMERIC,
            DataType::Decimal { .. } => SqlDataType::DECIMAL,
            DataType::Integer => SqlDataType::INTEGER,
            DataType::SmallInt => SqlDataType::SMALLINT,
            DataType::Float { .. } => SqlDataType::FLOAT,
            DataType::Real => SqlDataType::REAL,
            DataType::Double => SqlDataType::DOUBLE,
            DataType::Varchar { .. } => SqlDataType::VARCHAR,
            DataType::LongVarchar { .. } => SqlDataType::EXT_LONG_VARCHAR,
            DataType::Date => SqlDataType::DATE,
            DataType::Time { .. } => SqlDataType::TIME,
            DataType::Timestamp { .. } => SqlDataType::TIMESTAMP,
            DataType::BigInt => SqlDataType::EXT_BIG_INT,
            DataType::TinyInt => SqlDataType::EXT_TINY_INT,
            DataType::Bit => SqlDataType::EXT_BIT,
            DataType::WVarchar { .. } => SqlDataType::EXT_W_VARCHAR,
            DataType::WChar { .. } => SqlDataType::EXT_W_CHAR,
            DataType::Other { data_type, .. } => *data_type,
        }
    }

    // Return the column size, as it is required to bind the data type as a parameter. Fixed sized
    // types are mapped to `None` and should be bound using `0`. See also
    // [crates::Cursor::describe_col]. Variadic types without upper bound are also mapped to `None`.
    pub fn column_size(&self) -> Option<NonZeroUsize> {
        match self {
            DataType::Unknown
            | DataType::Integer
            | DataType::SmallInt
            | DataType::Real
            | DataType::Double
            | DataType::Date
            | DataType::Time { .. }
            | DataType::Timestamp { .. }
            | DataType::BigInt
            | DataType::TinyInt
            | DataType::Bit => None,
            DataType::Char { length }
            | DataType::Varchar { length }
            | DataType::Varbinary { length }
            | DataType::LongVarbinary { length }
            | DataType::Binary { length }
            | DataType::WChar { length }
            | DataType::WVarchar { length }
            | DataType::LongVarchar { length } => *length,
            DataType::Float { precision, .. }
            | DataType::Numeric { precision, .. }
            | DataType::Decimal { precision, .. } => NonZeroUsize::new(*precision),
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
            | DataType::Float { .. }
            | DataType::Real
            | DataType::Double
            | DataType::Varchar { .. }
            | DataType::WVarchar { .. }
            | DataType::WChar { .. }
            | DataType::Varbinary { .. }
            | DataType::LongVarbinary { .. }
            | DataType::Binary { .. }
            | DataType::LongVarchar { .. }
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
    pub fn display_size(&self) -> Option<NonZeroUsize> {
        match self {
            DataType::Unknown
            | DataType::Other {
                data_type: _,
                column_size: _,
                decimal_digits: _,
            } => None,
            // Each binary byte is represented by a 2-digit hexadecimal number.
            DataType::Varbinary { length }
            | DataType::Binary { length }
            | DataType::LongVarbinary { length } => {
                length.map(|l| l.get() * 2).and_then(NonZeroUsize::new)
            }
            // The defined (for fixed types) or maximum (for variable types) number of characters
            // needed to display the data in character form.
            DataType::Varchar { length }
            | DataType::WVarchar { length }
            | DataType::WChar { length }
            | DataType::Char { length }
            | DataType::LongVarchar { length } => *length,
            // The precision of the column plus 2 (a sign, precision digits, and a decimal point).
            // For example, the display size of a column defined as NUMERIC(10,3) is 12.
            DataType::Numeric {
                precision,
                scale: _,
            }
            | DataType::Decimal {
                precision,
                scale: _,
            } => NonZeroUsize::new(precision + 2),
            // 11 if signed (a sign and 10 digits) or 10 if unsigned (10 digits).
            DataType::Integer => NonZeroUsize::new(11),
            // 6 if signed (a sign and 5 digits) or 5 if unsigned (5 digits).
            DataType::SmallInt => NonZeroUsize::new(6),
            // 24 (a sign, 15 digits, a decimal point, the letter E, a sign, and 3 digits).
            DataType::Float { .. } | DataType::Double => NonZeroUsize::new(24),
            // 14 (a sign, 7 digits, a decimal point, the letter E, a sign, and 2 digits).
            DataType::Real => NonZeroUsize::new(14),
            // 10 (a date in the format yyyy-mm-dd).
            DataType::Date => NonZeroUsize::new(10),
            // 8 (a time in the format hh:mm:ss)
            // or
            // 9 + s (a time in the format hh:mm:ss[.fff...], where s is the fractional seconds
            // precision).
            DataType::Time { precision } => NonZeroUsize::new(if *precision == 0 {
                8
            } else {
                9 + *precision as usize
            }),
            // 19 (for a timestamp in the yyyy-mm-dd hh:mm:ss format)
            // or
            // 20 + s (for a timestamp in the yyyy-mm-dd hh:mm:ss[.fff...] format, where s is the
            // fractional seconds precision).
            DataType::Timestamp { precision } => NonZeroUsize::new(if *precision == 0 {
                19
            } else {
                20 + *precision as usize
            }),
            // 20 (a sign and 19 digits if signed or 20 digits if unsigned).
            DataType::BigInt => NonZeroUsize::new(20),
            // 4 if signed (a sign and 3 digits) or 3 if unsigned (3 digits).
            DataType::TinyInt => NonZeroUsize::new(4),
            // 1 digit.
            DataType::Bit => NonZeroUsize::new(1),
        }
    }

    /// The maximum length of the UTF-8 representation in bytes.
    ///
    /// ```
    /// use odbc_api::DataType;
    /// use std::num::NonZeroUsize;
    ///
    /// let nz = NonZeroUsize::new;
    /// // Character set data types length is multiplied by four.
    /// assert_eq!(DataType::Varchar { length: nz(10) }.utf8_len(), nz(40));
    /// assert_eq!(DataType::Char { length: nz(10) }.utf8_len(), nz(40));
    /// assert_eq!(DataType::WVarchar { length: nz(10) }.utf8_len(), nz(40));
    /// assert_eq!(DataType::WChar { length: nz(10) }.utf8_len(), nz(40));
    /// // For other types return value is identical to display size as they are assumed to be
    /// // entirely representable with ASCII characters.
    /// assert_eq!(DataType::Numeric { precision: 10, scale: 3}.utf8_len(), nz(10 + 2));
    /// ```
    pub fn utf8_len(&self) -> Option<NonZeroUsize> {
        match self {
            // One character may need up to four bytes to be represented in utf-8.
            DataType::Varchar { length }
            | DataType::WVarchar { length }
            | DataType::WChar { length }
            | DataType::Char { length } => length.map(|l| l.get() * 4).and_then(NonZeroUsize::new),
            other => other.display_size(),
        }
    }

    /// The maximum length of the UTF-16 representation in 2-Byte characters.
    ///
    /// ```
    /// use odbc_api::DataType;
    /// use std::num::NonZeroUsize;
    ///
    /// let nz = NonZeroUsize::new;
    ///
    /// // Character set data types length is multiplied by two.
    /// assert_eq!(DataType::Varchar { length: nz(10) }.utf16_len(), nz(20));
    /// assert_eq!(DataType::Char { length: nz(10) }.utf16_len(), nz(20));
    /// assert_eq!(DataType::WVarchar { length: nz(10) }.utf16_len(), nz(20));
    /// assert_eq!(DataType::WChar { length: nz(10) }.utf16_len(), nz(20));
    /// // For other types return value is identical to display size as they are assumed to be
    /// // entirely representable with ASCII characters.
    /// assert_eq!(DataType::Numeric { precision: 10, scale: 3}.utf16_len(), nz(10 + 2));
    /// ```
    pub fn utf16_len(&self) -> Option<NonZeroUsize> {
        match self {
            // One character may need up to two u16 to be represented in utf-16.
            DataType::Varchar { length }
            | DataType::WVarchar { length }
            | DataType::WChar { length }
            | DataType::Char { length } => length.map(|l| l.get() * 2).and_then(NonZeroUsize::new),
            other => other.display_size(),
        }
    }
}

use std::{char::REPLACEMENT_CHARACTER, convert::TryInto};

use odbc_sys::SqlDataType;
use widestring::decode_utf16;

use crate::{handles::Statement, ColumnDescription, DataType, Error};

/// Provides Metadata of the resulting the result set. Implemented by `Cursor` types and prepared
/// queries. Fetching metadata from a prepared query might be expensive (driver dependent), so your
/// application should fetch the Metadata it requires from the `Cursor` if possible.
///
/// See also:
/// <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/result-set-metadata>
pub trait ResultSetMetadata {
    /// Statement type of the cursor. This is always an instantiation of
    /// [`crate::handles::Statement`] with a generic parameter indicating the lifetime of the
    /// associated connection.
    ///
    /// So this trait could have had a lifetime parameter instead and provided access to the
    /// underlying type. However by using the projection of only the cursor methods of the
    /// underlying statement, consumers of this trait no only have to worry about the lifetime of
    /// the statement itself (e.g. the prepared query) and not about the lifetime of the connection
    /// it belongs to.
    type Statement: Statement;

    /// Get a shared reference to the underlying statement handle. This method is used to implement
    /// other more high level methods like [`Self::describe_col`] on top of it. It is usually not
    /// intended to be called by users of this library directly, but may serve as an escape hatch
    /// for low level usecases.
    fn stmt_ref(&self) -> &Self::Statement;

    /// Fetch a column description using the column index.
    ///
    /// # Parameters
    ///
    /// * `column_number`: Column index. `0` is the bookmark column. The other column indices start
    /// with `1`.
    /// * `column_description`: Holds the description of the column after the call. This method does
    /// not provide strong exception safety as the value of this argument is undefined in case of an
    /// error.
    fn describe_col(
        &self,
        column_number: u16,
        column_description: &mut ColumnDescription,
    ) -> Result<(), Error> {
        let stmt = self.stmt_ref();
        stmt.describe_col(column_number, column_description)
            .into_result(stmt)
    }

    /// Number of columns in result set. Can also be used to see wether execting a prepared
    /// Statement ([`crate::Prepared`]) would yield a result set, as this would return `0` if it
    /// does not.
    fn num_result_cols(&self) -> Result<i16, Error> {
        let stmt = self.stmt_ref();
        stmt.num_result_cols().into_result(stmt)
    }

    /// `true` if a given column in a result set is unsigned or not a numeric type, `false`
    /// otherwise.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn is_unsigned_column(&self, column_number: u16) -> Result<bool, Error> {
        let stmt = self.stmt_ref();
        stmt.is_unsigned_column(column_number).into_result(stmt)
    }

    /// Returns the size in bytes of the columns. For variable sized types the maximum size is
    /// returned, excluding a terminating zero.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_octet_length(&self, column_number: u16) -> Result<isize, Error> {
        let stmt = self.stmt_ref();
        stmt.col_octet_length(column_number).into_result(stmt)
    }

    /// Maximum number of characters required to display data from the column.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_display_size(&self, column_number: u16) -> Result<isize, Error> {
        let stmt = self.stmt_ref();
        stmt.col_display_size(column_number).into_result(stmt)
    }

    /// Precision of the column.
    ///
    /// Denotes the applicable precision. For data types SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP, and all
    /// the interval data types that represent a time interval, its value is the applicable
    /// precision of the fractional seconds component.
    fn col_precision(&self, column_number: u16) -> Result<isize, Error> {
        let stmt = self.stmt_ref();
        stmt.col_precision(column_number).into_result(stmt)
    }

    /// The applicable scale for a numeric data type. For DECIMAL and NUMERIC data types, this is
    /// the defined scale. It is undefined for all other data types.
    fn col_scale(&self, column_number: u16) -> Result<isize, Error> {
        let stmt = self.stmt_ref();
        stmt.col_scale(column_number).into_result(stmt)
    }

    /// The column alias, if it applies. If the column alias does not apply, the column name is
    /// returned. If there is no column name or a column alias, an empty string is returned.
    fn col_name(&self, column_number: u16, buf: &mut Vec<u16>) -> Result<(), Error> {
        let stmt = self.stmt_ref();
        stmt.col_name(column_number, buf).into_result(stmt)
    }

    /// Use this if you want to iterate over all column names and allocate a `String` for each one.
    ///
    /// This is a wrapper around `col_name` introduced for convenience.
    fn column_names(&self) -> Result<ColumnNamesIt<'_, Self>, Error> {
        ColumnNamesIt::new(self)
    }

    /// Data type of the specified column.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_data_type(&self, column_number: u16) -> Result<DataType, Error> {
        let stmt = self.stmt_ref();
        let kind = stmt.col_concise_type(column_number).into_result(stmt)?;
        let dt = match kind {
            SqlDataType::UNKNOWN_TYPE => DataType::Unknown,
            SqlDataType::EXT_VAR_BINARY => DataType::Varbinary {
                length: self.col_octet_length(column_number)?.try_into().unwrap(),
            },
            SqlDataType::EXT_LONG_VAR_BINARY => DataType::LongVarbinary {
                length: self.col_octet_length(column_number)?.try_into().unwrap(),
            },
            SqlDataType::EXT_BINARY => DataType::Binary {
                length: self.col_octet_length(column_number)?.try_into().unwrap(),
            },
            SqlDataType::EXT_W_VARCHAR => DataType::WVarchar {
                length: self.col_display_size(column_number)?.try_into().unwrap(),
            },
            SqlDataType::EXT_W_CHAR => DataType::WChar {
                length: self.col_display_size(column_number)?.try_into().unwrap(),
            },
            SqlDataType::EXT_LONG_VARCHAR => DataType::LongVarchar {
                length: self.col_display_size(column_number)?.try_into().unwrap(),
            },
            SqlDataType::CHAR => DataType::Char {
                length: self.col_display_size(column_number)?.try_into().unwrap(),
            },
            SqlDataType::VARCHAR => DataType::Varchar {
                length: self.col_display_size(column_number)?.try_into().unwrap(),
            },
            SqlDataType::NUMERIC => DataType::Numeric {
                precision: self.col_precision(column_number)?.try_into().unwrap(),
                scale: self.col_scale(column_number)?.try_into().unwrap(),
            },
            SqlDataType::DECIMAL => DataType::Decimal {
                precision: self.col_precision(column_number)?.try_into().unwrap(),
                scale: self.col_scale(column_number)?.try_into().unwrap(),
            },
            SqlDataType::INTEGER => DataType::Integer,
            SqlDataType::SMALLINT => DataType::SmallInt,
            SqlDataType::FLOAT => DataType::Float {
                precision: self.col_precision(column_number)?.try_into().unwrap(),
            },
            SqlDataType::REAL => DataType::Real,
            SqlDataType::DOUBLE => DataType::Double,
            SqlDataType::DATE => DataType::Date,
            SqlDataType::TIME => DataType::Time {
                precision: self.col_precision(column_number)?.try_into().unwrap(),
            },
            SqlDataType::TIMESTAMP => DataType::Timestamp {
                precision: self.col_precision(column_number)?.try_into().unwrap(),
            },
            SqlDataType::EXT_BIG_INT => DataType::BigInt,
            SqlDataType::EXT_TINY_INT => DataType::TinyInt,
            SqlDataType::EXT_BIT => DataType::Bit,
            other => {
                let mut column_description = ColumnDescription::default();
                self.describe_col(column_number, &mut column_description)?;
                DataType::Other {
                    data_type: other,
                    column_size: column_description.data_type.column_size(),
                    decimal_digits: column_description.data_type.decimal_digits(),
                }
            }
        };
        Ok(dt)
    }

    /// The maximum binary length of UTF-8 representation of elements of this column. You may
    /// specify the data type of the column to safe function calls into ODBC. Call this method to
    /// determine maxmimum string length for UTF-8 text buffers.
    ///
    /// This method assumes the encoding of text columns to be UTF-8.
    fn utf8_len(&self, column_number: u16, hint: Option<DataType>) -> Result<usize, Error> {
        let stmt = self.stmt_ref();
        let sql_type = if let Some(dt) = hint {
            dt.data_type()
        } else {
            stmt.col_concise_type(column_number).into_result(stmt)?
        };

        if is_narrow_text(sql_type) {
            // Query binary length
            return Ok(self.col_octet_length(column_number)?.try_into().unwrap());
        }

        if is_wide_text(sql_type) {
            // Utf16 to Utf8 conversion. We must adjust the binary size reported by the database,
            // since it assumes UTF-16 encoding. While usually an UTF-8 string is shorter for
            // characters in the range from U+0800 to U+FFFF UTF-8 is three bytes theras UTF-16 is
            // only two bytes large. We have to allocate enough memory to hold the largest possible
            // string.
            let octet_len: usize = self.col_octet_length(column_number)?.try_into().unwrap();
            // Utf16 to Utf8 conversion. We must adjust the binary size reported by the database,
            // since it assumes UTF-16 encoding. While usually an UTF-8 string is shorter for
            // characters in the range from U+0800 to U+FFFF UTF-8 is three bytes theras UTF-16 is
            // only two bytes large. We have to allocate enough memory to hold the largest possible
            // string.
            return Ok((octet_len / 2) * 3)
        }

        // Assume non text columns to be representable in ASCII. Look if we can display size
        // from hint, otherwise ask the driver.
        let binary_len = if let Some(display_size) = sql_type_to_display_size(sql_type) {
            display_size
        } else if let Some(display_size) = hint.and_then(|dt| dt.display_size()) {
            display_size
        } else {
            self.col_display_size(column_number)?.try_into().unwrap()
        };
        Ok(binary_len)
    }

    /// The maximum length of the UTF-16 representation in 2-Byte characters.
    ///
    /// You may specify the data type of the column to safe function calls into ODBC. Call this
    /// method to determine maxmimum string length for UTF-8 text buffers.
    fn utf16_len(&self, column_number: u16, hint: Option<DataType>) -> Result<usize, Error> {
        let stmt = self.stmt_ref();
        let sql_type = if let Some(dt) = hint {
            dt.data_type()
        } else {
            stmt.col_concise_type(column_number).into_result(stmt)?
        };

        if is_narrow_text(sql_type) {
            // Utf8 to Utf16 conversion. The letters in the range from U+0000 to U+007F take two
            // bytes in UTF-16 but only one byte in UTF-8. Since we return the length in `u16` the
            // value is divided and multiplied by two => the UTF-8 octet length is the UTF-16 len.
            return Ok(self.col_octet_length(column_number)?.try_into().unwrap());
        }

        if is_wide_text(sql_type) {
            let octet_len: usize = self.col_octet_length(column_number)?.try_into().unwrap();
            // Octet is the same as a byte. A `u16` consists of two bytes.
            return Ok(octet_len / 2)
        }

        // Assume non text columns to be representable in ASCII. Look if we can display size
        // from hint, otherwise ask the driver.
        let binary_len = if let Some(display_size) = sql_type_to_display_size(sql_type) {
            display_size
        } else if let Some(display_size) = hint.and_then(|dt| dt.display_size()) {
            display_size
        } else {
            self.col_display_size(column_number)?.try_into().unwrap()
        };
        Ok(binary_len)
    }
}

/// Should the information of the SQL type alone, be enough to determine the maximum display size
/// this function will return it.
fn sql_type_to_display_size(sql_type: SqlDataType) -> Option<usize> {
    let ret = match sql_type {
        // 11 if signed (a sign and 10 digits) or 10 if unsigned (10 digits).
        SqlDataType::INTEGER => 11,
        // 6 if signed (a sign and 5 digits) or 5 if unsigned (5 digits).
        SqlDataType::SMALLINT => 6,
        // 24 (a sign, 15 digits, a decimal point, the letter E, a sign, and 3 digits).
        SqlDataType::DOUBLE => 24,
        // 14 (a sign, 7 digits, a decimal point, the letter E, a sign, and 2 digits).
        SqlDataType::REAL => 14,
        // 10 (a date in the format yyyy-mm-dd).
        SqlDataType::DATE => 10,
        // 20 (a sign and 19 digits if signed or 20 digits if unsigned).
        SqlDataType::EXT_BIG_INT => 20,
        // 4 if signed (a sign and 3 digits) or 3 if unsigned (3 digits).
        SqlDataType::EXT_TINY_INT => 4,
        // 1 digit.
        SqlDataType::EXT_BIT => 1,
        _ => return None,
    };
    Some(ret)
}

/// True if the SQL data type is narrow text (usually utf8) on the DBC
fn is_narrow_text(sql_type: SqlDataType) -> bool {
    matches!(
        sql_type,
        SqlDataType::CHAR | SqlDataType::VARCHAR | SqlDataType::EXT_LONG_VARCHAR
    )
}

/// True if the SQL data type is wide text (utf16) on the DBC
fn is_wide_text(sql_type: SqlDataType) -> bool {
    matches!(
        sql_type,
        SqlDataType::EXT_W_CHAR | SqlDataType::EXT_W_VARCHAR | SqlDataType::EXT_W_LONG_VARCHAR
    )
}

/// An iterator calling `col_name` for each column_name and converting the result into UTF-8. See
/// [`ResultSetMetada::column_names`].
pub struct ColumnNamesIt<'c, C: ?Sized> {
    cursor: &'c C,
    buffer: Vec<u16>,
    column: u16,
    num_cols: u16,
}

impl<'c, C: ResultSetMetadata + ?Sized> ColumnNamesIt<'c, C> {
    fn new(cursor: &'c C) -> Result<Self, Error> {
        Ok(Self {
            cursor,
            // Some ODBC drivers do not report the required size to hold the column name. Starting
            // with a reasonable sized buffers, allows us to fetch reasonable sized column alias
            // even from those.
            buffer: Vec::with_capacity(128),
            num_cols: cursor.num_result_cols()?.try_into().unwrap(),
            column: 1,
        })
    }
}

impl<C> Iterator for ColumnNamesIt<'_, C>
where
    C: ResultSetMetadata,
{
    type Item = Result<String, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.column <= self.num_cols {
            let result = self
                .cursor
                .col_name(self.column, &mut self.buffer)
                .map(|()| {
                    decode_utf16(self.buffer.iter().copied())
                        .map(|decoding_result| decoding_result.unwrap_or(REPLACEMENT_CHARACTER))
                        .collect()
                });
            self.column += 1;
            Some(result)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let num_cols = self.num_cols as usize;
        (num_cols, Some(num_cols))
    }
}

impl<C> ExactSizeIterator for ColumnNamesIt<'_, C> where C: ResultSetMetadata {}

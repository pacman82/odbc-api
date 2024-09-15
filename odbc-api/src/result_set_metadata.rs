use std::num::NonZeroUsize;

use odbc_sys::SqlDataType;

use crate::{
    handles::{slice_to_utf8, AsStatementRef, SqlChar, Statement},
    ColumnDescription, DataType, Error,
};

/// Provides Metadata of the resulting the result set. Implemented by `Cursor` types and prepared
/// queries. Fetching metadata from a prepared query might be expensive (driver dependent), so your
/// application should fetch the Metadata it requires from the `Cursor` if possible.
///
/// See also:
/// <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/result-set-metadata>
pub trait ResultSetMetadata: AsStatementRef {
    /// Fetch a column description using the column index.
    ///
    /// # Parameters
    ///
    /// * `column_number`: Column index. `0` is the bookmark column. The other column indices start
    ///   with `1`.
    /// * `column_description`: Holds the description of the column after the call. This method does
    ///   not provide strong exception safety as the value of this argument is undefined in case of
    ///   an error.
    fn describe_col(
        &mut self,
        column_number: u16,
        column_description: &mut ColumnDescription,
    ) -> Result<(), Error> {
        let stmt = self.as_stmt_ref();
        stmt.describe_col(column_number, column_description)
            .into_result(&stmt)
    }

    /// Number of columns in result set. Can also be used to see whether executing a prepared
    /// Statement ([`crate::Prepared`]) would yield a result set, as this would return `0` if it
    /// does not.
    ///
    /// See also:
    /// <https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlnumresultcols-function>
    fn num_result_cols(&mut self) -> Result<i16, Error> {
        let stmt = self.as_stmt_ref();
        stmt.num_result_cols().into_result(&stmt)
    }

    /// `true` if a given column in a result set is unsigned or not a numeric type, `false`
    /// otherwise.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn column_is_unsigned(&mut self, column_number: u16) -> Result<bool, Error> {
        let stmt = self.as_stmt_ref();
        stmt.is_unsigned_column(column_number).into_result(&stmt)
    }

    /// Size in bytes of the columns. For variable sized types this is the maximum size, excluding a
    /// terminating zero.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_octet_length(&mut self, column_number: u16) -> Result<Option<NonZeroUsize>, Error> {
        let stmt = self.as_stmt_ref();
        stmt.col_octet_length(column_number)
            .into_result(&stmt)
            .map(|signed| NonZeroUsize::new(signed.max(0) as usize))
    }

    /// Maximum number of characters required to display data from the column. If the driver is
    /// unable to provide a maximum `None` is returned.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_display_size(&mut self, column_number: u16) -> Result<Option<NonZeroUsize>, Error> {
        let stmt = self.as_stmt_ref();
        stmt.col_display_size(column_number)
            .into_result(&stmt)
            // Map negative values to `0`. `0` is used by MSSQL to indicate a missing upper bound
            // `-4` (`NO_TOTAL`) is used by MySQL to do the same. Mapping them both to the same
            // value allows for less error prone generic applications. Making this value `None`
            // instead of zero makes it explicit, that an upper bound can not always be known. It
            // also prevents the order from being misunderstood, because the largest possible value
            // is obviously `> 0` in this case, yet `0` is smaller than any other value.
            .map(|signed| NonZeroUsize::new(signed.max(0) as usize))
    }

    /// Precision of the column.
    ///
    /// Denotes the applicable precision. For data types SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP, and all
    /// the interval data types that represent a time interval, its value is the applicable
    /// precision of the fractional seconds component.
    fn col_precision(&mut self, column_number: u16) -> Result<isize, Error> {
        let stmt = self.as_stmt_ref();
        stmt.col_precision(column_number).into_result(&stmt)
    }

    /// The applicable scale for a numeric data type. For DECIMAL and NUMERIC data types, this is
    /// the defined scale. It is undefined for all other data types.
    fn col_scale(&mut self, column_number: u16) -> Result<isize, Error> {
        let stmt = self.as_stmt_ref();
        stmt.col_scale(column_number).into_result(&stmt)
    }

    /// The column alias, if it applies. If the column alias does not apply, the column name is
    /// returned. If there is no column name or a column alias, an empty string is returned.
    fn col_name(&mut self, column_number: u16) -> Result<String, Error> {
        let stmt = self.as_stmt_ref();
        let mut buf = vec![0; 1024];
        stmt.col_name(column_number, &mut buf).into_result(&stmt)?;
        Ok(slice_to_utf8(&buf).unwrap())
    }

    /// Use this if you want to iterate over all column names and allocate a `String` for each one.
    ///
    /// This is a wrapper around `col_name` introduced for convenience.
    fn column_names(&mut self) -> Result<ColumnNamesIt<'_, Self>, Error> {
        ColumnNamesIt::new(self)
    }

    /// Data type of the specified column.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_data_type(&mut self, column_number: u16) -> Result<DataType, Error> {
        let stmt = self.as_stmt_ref();
        let kind = stmt.col_concise_type(column_number).into_result(&stmt)?;
        let dt = match kind {
            SqlDataType::UNKNOWN_TYPE => DataType::Unknown,
            SqlDataType::EXT_VAR_BINARY => DataType::Varbinary {
                length: self.col_octet_length(column_number)?,
            },
            SqlDataType::EXT_LONG_VAR_BINARY => DataType::LongVarbinary {
                length: self.col_octet_length(column_number)?,
            },
            SqlDataType::EXT_BINARY => DataType::Binary {
                length: self.col_octet_length(column_number)?,
            },
            SqlDataType::EXT_W_VARCHAR => DataType::WVarchar {
                length: self.col_display_size(column_number)?,
            },
            SqlDataType::EXT_W_CHAR => DataType::WChar {
                length: self.col_display_size(column_number)?,
            },
            SqlDataType::EXT_LONG_VARCHAR => DataType::LongVarchar {
                length: self.col_display_size(column_number)?,
            },
            SqlDataType::CHAR => DataType::Char {
                length: self.col_display_size(column_number)?,
            },
            SqlDataType::VARCHAR => DataType::Varchar {
                length: self.col_display_size(column_number)?,
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
}

/// Buffer sizes able to hold the display size of each column in utf-8 encoding. You may call this
/// method to figure out suitable buffer sizes for text columns. [`buffers::TextRowSet::for_cursor`]
/// will invoke this function for you.
///
/// # Parameters
///
/// * `metadata`: Used to query the display size for each column of the row set. For character
///   data the length in characters is multiplied by 4 in order to have enough space for 4 byte
///   utf-8 characters. This is a pessimization for some data sources (e.g. SQLite 3) which do
///   interpret the size of a `VARCHAR(5)` column as 5 bytes rather than 5 characters.
pub fn utf8_display_sizes(
    metadata: &mut impl ResultSetMetadata,
) -> Result<impl Iterator<Item = Result<Option<NonZeroUsize>, Error>> + '_, Error> {
    let num_cols: u16 = metadata.num_result_cols()?.try_into().unwrap();
    let it = (1..(num_cols + 1)).map(move |col_index| {
        // Ask driver for buffer length
        let max_str_len = if let Some(encoded_len) = metadata.col_data_type(col_index)?.utf8_len() {
            Some(encoded_len)
        } else {
            metadata.col_display_size(col_index)?
        };
        Ok(max_str_len)
    });
    Ok(it)
}

/// An iterator calling `col_name` for each column_name and converting the result into UTF-8. See
/// [`ResultSetMetada::column_names`].
pub struct ColumnNamesIt<'c, C: ?Sized> {
    cursor: &'c mut C,
    buffer: Vec<SqlChar>,
    column: u16,
    num_cols: u16,
}

impl<'c, C: ResultSetMetadata + ?Sized> ColumnNamesIt<'c, C> {
    fn new(cursor: &'c mut C) -> Result<Self, Error> {
        let num_cols = cursor.num_result_cols()?.try_into().unwrap();
        Ok(Self {
            cursor,
            // Some ODBC drivers do not report the required size to hold the column name. Starting
            // with a reasonable sized buffers, allows us to fetch reasonable sized column alias
            // even from those.
            buffer: Vec::with_capacity(128),
            num_cols,
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
            // stmt instead of cursor.col_name, so we can efficently reuse the buffer and avoid
            // extra allocations.
            let stmt = self.cursor.as_stmt_ref();

            let result = stmt
                .col_name(self.column, &mut self.buffer)
                .into_result(&stmt)
                .map(|()| slice_to_utf8(&self.buffer).unwrap());
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

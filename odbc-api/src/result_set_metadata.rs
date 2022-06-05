use odbc_sys::SqlDataType;

use crate::{
    handles::{slice_to_utf8, SqlChar, Statement, StatementRef},
    ColumnDescription, DataType, Error,
};

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

    /// Get an exclusive reference to the underlying statement handle. This method is used to
    /// implement other more high level methods like [`Self::describe_col`] on top of it. It is
    /// usually not intended to be called by users of this crate directly, but may serve as an
    /// escape hatch for low level usecases.
    fn as_stmt_ref(&mut self) -> StatementRef<'_>;

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
        &mut self,
        column_number: u16,
        column_description: &mut ColumnDescription,
    ) -> Result<(), Error> {
        let stmt = self.as_stmt_ref();
        stmt.describe_col(column_number, column_description)
            .into_result(&stmt)
    }

    /// Number of columns in result set. Can also be used to see wether execting a prepared
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

    /// Returns the size in bytes of the columns. For variable sized types the maximum size is
    /// returned, excluding a terminating zero.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_octet_length(&mut self, column_number: u16) -> Result<isize, Error> {
        let stmt = self.as_stmt_ref();
        stmt.col_octet_length(column_number).into_result(&stmt)
    }

    /// Maximum number of characters required to display data from the column.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_display_size(&mut self, column_number: u16) -> Result<isize, Error> {
        let stmt = self.as_stmt_ref();
        stmt.col_display_size(column_number).into_result(&stmt)
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

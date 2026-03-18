use std::num::NonZeroUsize;

use crate::{
    ColumnarBulkInserter, CursorImpl, DataType, Error, InputParameterMapping,
    ParameterCollectionRef, ResultSetMetadata,
    buffers::{AnyBuffer, BufferDesc, ColumnBuffer, TextColumn},
    columnar_bulk_inserter::InOrder,
    execute::execute_with_parameters,
    handles::{
        ASSUMED_MAX_LENGTH_OF_VARCHAR, ASSUMED_MAX_LENGTH_OF_W_VARCHAR, AsStatementRef, ColumnType,
        HasDataType, Statement, StatementRef,
    },
    parameter::WithDataType,
};

/// A prepared query. Prepared queries are useful if the similar queries should executed more than
/// once. See [`crate::Connection::prepare`].
pub struct Prepared<S> {
    statement: S,
}

impl<S> Prepared<S> {
    pub(crate) fn new(statement: S) -> Self {
        Self { statement }
    }

    /// Transfer ownership to the underlying statement handle.
    ///
    /// The resulting type is one level of indirection away from the raw pointer of the ODBC API. It
    /// no longer has any guarantees about bound buffers, but is still guaranteed to be a valid
    /// allocated statement handle. This serves together with
    /// [`crate::handles::StatementImpl::into_sys`] or [`crate::handles::Statement::as_sys`] this
    /// serves as an escape hatch to access the functionality provided by `crate::sys` not yet
    /// accessible through safe abstractions.
    pub fn into_handle(self) -> S {
        self.statement
    }
}

impl<S> Prepared<S>
where
    S: AsStatementRef,
{
    /// Execute the prepared statement.
    ///
    /// * `params`: Used to bind these parameters before executing the statement. You can use `()`
    ///   to represent no parameters. In regards to binding arrays of parameters: Should `params`
    ///   specify a parameter set size of `0`, nothing is executed, and `Ok(None)` is returned. See
    ///   the [`crate::parameter`] module level documentation for more information on how to pass
    ///   parameters.
    pub fn execute(
        &mut self,
        params: impl ParameterCollectionRef,
    ) -> Result<Option<CursorImpl<StatementRef<'_>>>, Error> {
        let stmt = self.statement.as_stmt_ref();
        execute_with_parameters(stmt, None, params)
    }

    /// Describes parameter marker associated with a prepared SQL statement.
    ///
    /// # Parameters
    ///
    /// * `parameter_number`: Parameter marker number ordered sequentially in increasing parameter
    ///   order, starting at 1.
    pub fn describe_param(&mut self, parameter_number: u16) -> Result<ColumnType, Error> {
        let mut stmt = self.as_stmt_ref();

        stmt.describe_param(parameter_number).into_result(&stmt)
    }

    /// Number of placeholders which must be provided with [`Self::execute`] in order to execute
    /// this statement. This is equivalent to the number of placeholders used in the SQL string
    /// used to prepare the statement.
    pub fn num_params(&mut self) -> Result<u16, Error> {
        let mut stmt = self.as_stmt_ref();
        stmt.num_params().into_result(&stmt)
    }

    /// Number of placeholders which must be provided with [`Self::execute`] in order to execute
    /// this statement. This is equivalent to the number of placeholders used in the SQL string
    /// used to prepare the statement.
    ///
    /// ```
    /// use odbc_api::{Connection, Error, handles::ColumnType};
    ///
    /// fn collect_parameter_descriptions(
    ///     connection: Connection<'_>
    /// ) -> Result<Vec<ColumnType>, Error>{
    ///     // Note the two `?` used as placeholders for the parameters.
    ///     let sql = "INSERT INTO NationalDrink (country, drink) VALUES (?, ?)";
    ///     let mut prepared = connection.prepare(sql)?;
    ///
    ///     let params: Vec<_> = prepared.parameter_descriptions()?.collect::<Result<_,_>>()?;
    ///
    ///     Ok(params)
    /// }
    /// ```
    pub fn parameter_descriptions(
        &mut self,
    ) -> Result<
        impl DoubleEndedIterator<Item = Result<ColumnType, Error>>
        + ExactSizeIterator<Item = Result<ColumnType, Error>>
        + '_,
        Error,
    > {
        Ok((1..=self.num_params()?).map(|index| self.describe_param(index)))
    }

    /// Unless you want to roll your own column buffer implementation users are encouraged to use
    /// [`Self::into_text_inserter`] or [`Self::into_column_inserter`] instead.
    ///
    /// # Safety
    ///
    /// * Parameters must all be valid for insertion. An example for an invalid parameter would be a
    ///   text buffer with a cell those indiactor value exceeds the maximum element length. This can
    ///   happen after when truncation occurs then writing into a buffer.
    pub unsafe fn unchecked_bind_columnar_array_parameters<C>(
        self,
        parameter_buffers: Vec<C>,
        index_mapping: impl InputParameterMapping,
    ) -> Result<ColumnarBulkInserter<S, C>, Error>
    where
        C: ColumnBuffer + HasDataType + Send,
    {
        // We know that statement is a prepared statement.
        unsafe { ColumnarBulkInserter::new(self.into_handle(), parameter_buffers, index_mapping) }
    }

    /// Use this to insert rows of string input into the database.
    ///
    /// ```
    /// use odbc_api::{Connection, Error};
    ///
    /// fn insert_text<'e>(connection: Connection<'e>) -> Result<(), Error>{
    ///     // Insert six rows of text with two columns each into the database in batches of 3. In a
    ///     // real use case you are likely to achieve a better results with a higher batch size.
    ///
    ///     // Note the two `?` used as placeholders for the parameters.
    ///     let prepared = connection.prepare("INSERT INTO NationalDrink (country, drink) VALUES (?, ?)")?;
    ///     // We assume both parameter inputs never exceed 50 bytes.
    ///     let mut prebound = prepared.into_text_inserter(3, [50, 50])?;
    ///
    ///     // A cell is an option to byte. We could use `None` to represent NULL but we have no
    ///     // need to do that in this example.
    ///     let as_cell = |s: &'static str| { Some(s.as_bytes()) } ;
    ///
    ///     // First batch of values
    ///     prebound.append(["England", "Tea"].into_iter().map(as_cell))?;
    ///     prebound.append(["Germany", "Beer"].into_iter().map(as_cell))?;
    ///     prebound.append(["Russia", "Vodka"].into_iter().map(as_cell))?;
    ///
    ///     // Execute statement using values bound in buffer.
    ///     prebound.execute()?;
    ///     // Clear buffer contents, otherwise the previous values would stay in the buffer.
    ///     prebound.clear();
    ///
    ///     // Second batch of values
    ///     prebound.append(["India", "Tea"].into_iter().map(as_cell))?;
    ///     prebound.append(["France", "Wine"].into_iter().map(as_cell))?;
    ///     prebound.append(["USA", "Cola"].into_iter().map(as_cell))?;
    ///
    ///     // Send second batch to the database
    ///     prebound.execute()?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn into_text_inserter(
        self,
        capacity: usize,
        max_str_len: impl IntoIterator<Item = usize>,
    ) -> Result<ColumnarBulkInserter<S, TextColumn<u8>>, Error> {
        let max_str_len = max_str_len.into_iter();
        let parameter_buffers: Vec<_> = max_str_len
            .map(|max_str_len| TextColumn::new(capacity, max_str_len))
            .collect();
        let index_mapping = InOrder::new(parameter_buffers.len());
        // Text Columns are created with NULL as default, which is valid for insertion.
        unsafe { self.unchecked_bind_columnar_array_parameters(parameter_buffers, index_mapping) }
    }

    /// A [`crate::ColumnarBulkInserter`] which takes ownership of both the statement and the bound
    /// array parameter buffers.
    ///
    /// ```no_run
    /// use odbc_api::{Connection, Error, IntoParameter, BindParamDesc};
    ///
    /// fn insert_birth_years(
    ///     conn: &Connection,
    ///     names: &[&str],
    ///     years: &[i16]
    /// ) -> Result<(), Error> {
    ///     // All columns must have equal length.
    ///     assert_eq!(names.len(), years.len());
    ///
    ///     let prepared = conn.prepare("INSERT INTO Birthdays (name, year) VALUES (?, ?)")?;
    ///
    ///     // Create a columnar buffer which fits the input parameters.
    ///     let parameter_descriptions = [
    ///         BindParamDesc::text(255),
    ///         BindParamDesc::i16(false),
    ///     ];
    ///     // The capacity must be able to hold at least the largest batch. We do everything in one
    ///     // go, so we set it to the length of the input parameters.
    ///     let capacity = names.len();
    ///     // Allocate memory for the array column parameters and bind it to the statement.
    ///     let mut prebound = prepared.into_column_inserter(capacity, parameter_descriptions)?;
    ///     // Length of this batch
    ///     prebound.set_num_rows(capacity);
    ///
    ///
    ///     // Fill the buffer with values column by column
    ///     let mut col = prebound
    ///         .column_mut(0)
    ///         .as_text_view()
    ///         .expect("We know the name column to hold text.");
    ///
    ///     for (index, name) in names.iter().enumerate() {
    ///         col.set_cell(index, Some(name.as_bytes()));
    ///     }
    ///
    ///     let col = prebound
    ///         .column_mut(1)
    ///         .as_slice::<i16>()
    ///         .expect("We know the year column to hold i16.");
    ///     col.copy_from_slice(years);
    ///
    ///     prebound.execute()?;
    ///     Ok(())
    /// }
    /// ```
    pub fn into_column_inserter(
        self,
        capacity: usize,
        descriptions: impl IntoIterator<Item = BindParamDesc>,
    ) -> Result<ColumnarBulkInserter<S, WithDataType<AnyBuffer>>, Error> {
        let parameter_buffers: Vec<_> = descriptions
            .into_iter()
            .map(|desc| desc.make_input_buffer(capacity))
            .collect();
        let index_mapping = InOrder::new(parameter_buffers.len());
        // Safe: We know this to be a valid prepared statement. Also we just created the buffers
        // to be bound and know them to be empty. => Therfore they are valid and do not contain any
        // indicator values which would could trigger out of bounds in the database drivers.
        unsafe { self.unchecked_bind_columnar_array_parameters(parameter_buffers, index_mapping) }
    }

    /// Similar to [`Self::into_column_inserter`], but allows to specify a custom mapping between
    /// columns and parameters. This is useful if e.g. the same values a bound to multiple
    /// parameter placeholders.
    pub fn into_column_inserter_with_mapping(
        self,
        capacity: usize,
        descriptions: impl IntoIterator<Item = BindParamDesc>,
        index_mapping: impl InputParameterMapping,
    ) -> Result<ColumnarBulkInserter<S, WithDataType<AnyBuffer>>, Error> {
        let parameter_buffers: Vec<_> = descriptions
            .into_iter()
            .map(|desc| desc.make_input_buffer(capacity))
            .collect();
        // Safe: We know this to be a valid prepared statement. Also we just created the buffers
        // to be bound and know them to be empty. => Therfore they are valid and do not contain any
        // indicator values which would could trigger out of bounds in the database drivers.
        unsafe { self.unchecked_bind_columnar_array_parameters(parameter_buffers, index_mapping) }
    }

    /// A [`crate::ColumnarBulkInserter`] which has ownership of the bound array parameter buffers
    /// and borrows the statement. For most usecases [`Self::into_column_inserter`] is what you
    /// want to use, yet on some instances you may want to bind new paramater buffers to the same
    /// prepared statement. E.g. to grow the capacity dynamically during insertions with several
    /// chunks. In such use cases you may only want to borrow the prepared statemnt, so it can be
    /// reused with a different set of parameter buffers.
    pub fn column_inserter(
        &mut self,
        capacity: usize,
        descriptions: impl IntoIterator<Item = BindParamDesc>,
    ) -> Result<ColumnarBulkInserter<StatementRef<'_>, WithDataType<AnyBuffer>>, Error> {
        // Remark: We repeat the implementation here. It is hard to reuse the
        // `column_inserter_with_mapping` function, because we need to know the number of parameters
        // to create the `InOrder` mapping.
        let stmt = self.statement.as_stmt_ref();

        let parameter_buffers: Vec<_> = descriptions
            .into_iter()
            .map(|desc| desc.make_input_buffer(capacity))
            .collect();

        let index_mapping = InOrder::new(parameter_buffers.len());
        // Safe: We know that the statement is a prepared statement, and we just created the buffers
        // to be bound and know them to be empty. => Therfore they are valid and do not contain any
        // indicator values which would could trigger out of bounds in the database drivers.
        unsafe { ColumnarBulkInserter::new(stmt, parameter_buffers, index_mapping) }
    }

    /// Similar to [`Self::column_inserter`], but allows to specify a custom mapping between columns
    /// and parameters. This is useful if e.g. the same values a bound to multiple parameter
    /// placeholders.
    pub fn column_inserter_with_mapping(
        &mut self,
        capacity: usize,
        descriptions: impl IntoIterator<Item = BindParamDesc>,
        index_mapping: impl InputParameterMapping,
    ) -> Result<ColumnarBulkInserter<StatementRef<'_>, WithDataType<AnyBuffer>>, Error> {
        let stmt = self.statement.as_stmt_ref();

        let parameter_buffers: Vec<_> = descriptions
            .into_iter()
            .map(|desc| desc.make_input_buffer(capacity))
            .collect();

        // Safe: We know that the statement is a prepared statement, and we just created the buffers
        // to be bound and know them to be empty. => Therfore they are valid and do not contain any
        // indicator values which would could trigger out of bounds in the database drivers.
        unsafe { ColumnarBulkInserter::new(stmt, parameter_buffers, index_mapping) }
    }

    /// Number of rows affected by the last `INSERT`, `UPDATE` or `DELETE` statement. May return
    /// `None` if row count is not available. Some drivers may also allow to use this to determine
    /// how many rows have been fetched using `SELECT`. Most drivers however only know how many rows
    /// have been fetched after they have been fetched.
    ///
    /// ```
    /// use odbc_api::{Connection, Error, IntoParameter};
    ///
    /// /// Deletes all comments for every user in the slice. Returns the number of deleted
    /// /// comments.
    /// pub fn delete_all_comments_from(
    ///     users: &[&str],
    ///     conn: Connection<'_>,
    /// ) -> Result<usize, Error>
    /// {
    ///     // Store prepared query for fast repeated execution.
    ///     let mut prepared = conn.prepare("DELETE FROM Comments WHERE user=?")?;
    ///     let mut total_deleted_comments = 0;
    ///     for user in users {
    ///         prepared.execute(&user.into_parameter())?;
    ///         total_deleted_comments += prepared
    ///             .row_count()?
    ///             .expect("Row count must always be available for DELETE statements.");
    ///     }
    ///     Ok(total_deleted_comments)
    /// }
    /// ```
    pub fn row_count(&mut self) -> Result<Option<usize>, Error> {
        let mut stmt = self.statement.as_stmt_ref();
        stmt.row_count().into_result(&stmt).map(|count| {
            // ODBC returns -1 in case a row count is not available
            if count == -1 {
                None
            } else {
                Some(count.try_into().unwrap())
            }
        })
    }

    /// Use this to limit the time the query is allowed to take, before responding with data to the
    /// application. The driver may replace the number of seconds you provide with a minimum or
    /// maximum value. You can specify ``0``, to deactivate the timeout, this is the default. For
    /// this to work the driver must support this feature. E.g. PostgreSQL, and Microsoft SQL Server
    /// do, but SQLite or MariaDB do not.
    ///
    /// This corresponds to `SQL_ATTR_QUERY_TIMEOUT` in the ODBC C API.
    ///
    /// See:
    /// <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetstmtattr-function>
    pub fn set_query_timeout_sec(&mut self, timeout_sec: usize) -> Result<(), Error> {
        let mut stmt = self.statement.as_stmt_ref();
        stmt.set_query_timeout_sec(timeout_sec).into_result(&stmt)
    }

    /// The number of seconds to wait for a SQL statement to execute before returning to the
    /// application. If `timeout_sec` is equal to 0 (default), there is no timeout.
    ///
    /// This corresponds to `SQL_ATTR_QUERY_TIMEOUT` in the ODBC C API.
    ///
    /// See:
    /// <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetstmtattr-function>
    pub fn query_timeout_sec(&mut self) -> Result<usize, Error> {
        let mut stmt = self.statement.as_stmt_ref();
        stmt.query_timeout_sec().into_result(&stmt)
    }
}

/// Description of paramater domain and buffer bound for bulk insertion. Used by
/// [`Prepared::column_inserter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindParamDesc {
    /// Describes the C-Type used to describe values and wether we need a NULL representation.
    pub buffer_desc: BufferDesc,
    /// The relational type or domain of the parameter. While there is a strong correllation between
    /// the buffer used and the relational type, it is not always a naive mapping. E.g. a text buffer
    /// can not only carry VARCHAR but also a DECIMAL or a timestamp.
    pub data_type: DataType,
}

impl BindParamDesc {
    /// A description for binding utf-16 data to a parameter.
    ///
    /// # Parameters
    ///
    /// * `max_str_len`: The maximum length of the string in utf-16 code units. Excluding
    ///   terminating null character.
    ///
    /// Uses a wide character buffer and sets the data type to [`DataType::WVarchar`] or
    /// [`DataType::WLongVarchar`].
    pub fn wide_text(max_str_len: usize) -> Self {
        let data_type = if max_str_len <= ASSUMED_MAX_LENGTH_OF_W_VARCHAR {
            DataType::WVarchar {
                length: NonZeroUsize::new(max_str_len),
            }
        } else {
            DataType::WLongVarchar {
                length: NonZeroUsize::new(max_str_len),
            }
        };
        BindParamDesc {
            buffer_desc: BufferDesc::WText { max_str_len },
            data_type,
        }
    }

    /// A description for binding narrow text (usually utf-8) data to a parameter.
    ///
    /// * `max_str_len`: The maximum length of the string in bytes. excluding terminating null
    ///   character.
    ///
    /// Uses a narrow character buffer and sets the data type to [`DataType::Varchar`] or
    /// [`DataType::LongVarchar`].
    pub fn text(max_str_len: usize) -> Self {
        let data_type = if max_str_len <= ASSUMED_MAX_LENGTH_OF_VARCHAR {
            DataType::Varchar {
                length: NonZeroUsize::new(max_str_len),
            }
        } else {
            DataType::LongVarchar {
                length: NonZeroUsize::new(max_str_len),
            }
        };
        BindParamDesc {
            buffer_desc: BufferDesc::Text { max_str_len },
            data_type,
        }
    }

    /// A description for binding timestamps to a parameter.
    ///
    /// # Parameters
    ///
    /// * `nullable`: Whether the parameter can be NULL. If `true` null values can be represented,
    ///   if `false` null values can not be represented, but we can save an allocation for an
    ///   indicator buffer.
    /// * `precision`: The number of digits for the fractional seconds part. E.g. if you know your
    ///   input to be milliseconds choose `3`. Many databases error if you exceed the maximum
    ///   precision of the column. If you are unsure about the maximum precision supported by the
    ///   Database `7` is a good guess.
    pub fn timestamp(nullable: bool, precision: i16) -> Self {
        BindParamDesc {
            buffer_desc: BufferDesc::Timestamp { nullable },
            data_type: DataType::Timestamp { precision },
        }
    }

    /// A description for binding [`crate::Time`] values to a parameter.
    ///
    /// # Parameters
    ///
    /// * `nullable`: Whether the parameter can be NULL. If `true` null values can be represented,
    ///   if `false` null values can not be represented, but we can save an allocation for an
    ///   indicator buffer.
    pub fn time(nullable: bool) -> Self {
        BindParamDesc {
            buffer_desc: BufferDesc::Time { nullable },
            data_type: DataType::Time { precision: 0 },
        }
    }

    /// A description for binding wallclock time in a text buffer.
    ///
    /// # Parameters
    ///
    /// * `precision`: The number of digits for the fractional seconds part. E.g. if you know your
    ///   input to be milliseconds choose `3`. Some databases error if you exceed the maximum
    ///   precision of the column. If you are unsure about the maximum precision supported by the
    ///   Database `7` is a good guess.
    pub fn time_as_text(precision: i16) -> Self {
        // Text representation of time has a fixed length of 8 (hh:mm:ss) plus the radix character
        // and the fractional seconds. E.g. for milliseconds we would have `hh:mm:ss.fff` which has
        // a length of 12.
        let max_str_len = 8 + if precision > 0 {
            // Radix character + fractional seconds digits
            1 + precision as usize
        } else {
            0
        };
        BindParamDesc {
            buffer_desc: BufferDesc::Text { max_str_len },
            data_type: DataType::Time { precision },
        }
    }

    /// A description for binding decimal represented as text to a parameter.
    ///
    /// # Parameters
    ///
    /// * `precision`: The total number of digits in the decimal number. E.g. for `123.45` this
    ///   would be `5`.
    /// * `scale`: The number of digits to the right of the decimal point. E.g. for `123.45` this
    ///   would be `2`.
    ///
    pub fn decimal_as_text(precision: u8, scale: i8) -> Self {
        // Length of a text representation of a decimal
        let max_str_len = match scale {
            // Precision digits + (- scale zeroes) + sign
            i8::MIN..=-1 => (precision as i32 - scale as i32 + 1).try_into().unwrap(),
            // Precision digits + sign
            0 => precision as usize + 1,
            // Precision digits + radix character (`.`) + sign
            1.. => precision as usize + 1 + 1,
        };
        BindParamDesc {
            buffer_desc: BufferDesc::Text { max_str_len },
            data_type: DataType::Decimal {
                precision: precision as usize,
                scale: scale as i16,
            },
        }
    }

    /// A description for binding 16 bit integers to a parameter.
    ///
    /// # Parameters
    ///
    /// * `nullable`: Whether the parameter can be NULL. If `true` null values can be represented,
    ///   if `false` null values can not be represented, but we can save an allocation for an
    ///   indicator buffer.
    pub fn i16(nullable: bool) -> Self {
        BindParamDesc {
            buffer_desc: BufferDesc::I16 { nullable },
            data_type: DataType::SmallInt,
        }
    }

    /// A description for binding 32 bit integers to a parameter.
    ///
    /// # Parameters
    ///
    /// * `nullable`: Whether the parameter can be NULL. If `true` null values can be represented,
    ///   if `false` null values can not be represented, but we can save an allocation for an
    ///   indicator buffer.
    pub fn i32(nullable: bool) -> Self {
        BindParamDesc {
            buffer_desc: BufferDesc::I32 { nullable },
            data_type: DataType::Integer,
        }
    }

    /// A description for binding 64 bit integers to a parameter.
    ///
    /// # Parameters
    ///
    /// * `nullable`: Whether the parameter can be NULL. If `true` null values can be represented,
    ///   if `false` null values can not be represented, but we can save an allocation for an
    ///   indicator buffer.
    pub fn i64(nullable: bool) -> Self {
        BindParamDesc {
            buffer_desc: BufferDesc::I64 { nullable },
            data_type: DataType::BigInt,
        }
    }

    /// A description for binding variadic binary data to a parameter.
    ///
    /// # Parameters
    ///
    /// * `max_bytes`: The maximum length of the binary data in bytes.
    pub fn binary(max_bytes: usize) -> Self {
        BindParamDesc {
            buffer_desc: BufferDesc::Binary { max_bytes },
            data_type: DataType::Binary {
                length: NonZeroUsize::new(max_bytes),
            },
        }
    }

    /// A description for binding `f64` values to a parameter.
    ///
    /// # Parameters
    ///
    /// * `nullable`: Whether the parameter can be NULL. If `true` null values can be represented,
    ///   if `false` null values can not be represented, but we can save an allocation for an
    ///   indicator buffer.
    pub fn f64(nullable: bool) -> Self {
        BindParamDesc {
            buffer_desc: BufferDesc::F64 { nullable },
            data_type: DataType::Double,
        }
    }

    /// A description for binding `f32` values to a parameter.
    ///
    /// # Parameters
    ///
    /// * `nullable`: Whether the parameter can be NULL. If `true` null values can be represented,
    ///   if `false` null values can not be represented, but we can save an allocation for an
    ///   indicator buffer.
    pub fn f32(nullable: bool) -> Self {
        BindParamDesc {
            buffer_desc: BufferDesc::F32 { nullable },
            data_type: DataType::Real,
        }
    }

    /// A description for binding `u8` values to a parameter.
    ///
    /// # Parameters
    ///
    /// * `nullable`: Whether the parameter can be NULL. If `true` null values can be represented,
    ///   if `false` null values can not be represented, but we can save an allocation for an
    ///   indicator buffer.
    pub fn u8(nullable: bool) -> Self {
        BindParamDesc {
            buffer_desc: BufferDesc::U8 { nullable },
            // Few databases support unsigned types, binding U8 as tiny int might lead to weird
            // stuff if the database has type is signed. I guess. Let's bind it as SmallInt by
            // default, just to be on the safe side.
            data_type: DataType::SmallInt,
        }
    }

    /// A description for binding `u8` values to a parameter.
    ///
    /// # Parameters
    ///
    /// * `nullable`: Whether the parameter can be NULL. If `true` null values can be represented,
    ///   if `false` null values can not be represented, but we can save an allocation for an
    ///   indicator buffer.
    pub fn i8(nullable: bool) -> Self {
        BindParamDesc {
            buffer_desc: BufferDesc::I8 { nullable },
            data_type: DataType::TinyInt,
        }
    }

    /// A description for binding [`crate::sys::Date`] values to a parameter.
    ///
    /// # Parameters
    ///
    /// * `nullable`: Whether the parameter can be NULL. If `true` null values can be represented,
    ///   if `false` null values can not be represented, but we can save an allocation for an
    ///   indicator buffer.
    pub fn date(nullable: bool) -> Self {
        BindParamDesc {
            buffer_desc: BufferDesc::Date { nullable },
            data_type: DataType::Date,
        }
    }

    /// A description for binding [`crate::Bit`] values to a parameter.
    ///
    /// # Parameters
    ///
    /// * `nullable`: Whether the parameter can be NULL. If `true` null values can be represented,
    ///   if `false` null values can not be represented, but we can save an allocation for an
    ///   indicator buffer.
    pub fn bit(nullable: bool) -> Self {
        BindParamDesc {
            buffer_desc: BufferDesc::Bit { nullable },
            data_type: DataType::Bit,
        }
    }

    fn make_input_buffer(&self, max_rows: usize) -> WithDataType<AnyBuffer> {
        let buffer = AnyBuffer::from_desc(max_rows, self.buffer_desc);
        WithDataType::new(buffer, self.data_type)
    }
}

impl<S> ResultSetMetadata for Prepared<S> where S: AsStatementRef {}

impl<S> AsStatementRef for Prepared<S>
where
    S: AsStatementRef,
{
    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        self.statement.as_stmt_ref()
    }
}

#[cfg(test)]
mod tests {
    use crate::buffers::BufferDesc;

    use super::BindParamDesc;

    #[test]
    fn bind_paramater_description_decimal_length_positive_scale() {
        let BufferDesc::Text { max_str_len } = BindParamDesc::decimal_as_text(5, 2).buffer_desc
        else {
            panic!("Expected a text buffer for decimal parameters.")
        };
        assert_eq!("-123.45".len(), max_str_len);
    }

    #[test]
    fn bind_paramater_description_decimal_length_scale_zero() {
        let BufferDesc::Text { max_str_len } = BindParamDesc::decimal_as_text(5, 0).buffer_desc
        else {
            panic!("Expected a text buffer for decimal parameters.")
        };
        assert_eq!("-12345".len(), max_str_len);
    }

    #[test]
    fn bind_paramater_description_decimal_length_negative_scale() {
        let BufferDesc::Text { max_str_len } = BindParamDesc::decimal_as_text(5, -2).buffer_desc
        else {
            panic!("Expected a text buffer for decimal parameters.")
        };
        assert_eq!("-1234500".len(), max_str_len);
    }
}

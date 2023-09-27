use crate::{
    buffers::{AnyBuffer, BufferDesc, ColumnBuffer, TextColumn},
    execute::execute_with_parameters,
    handles::{AsStatementRef, HasDataType, ParameterDescription, Statement, StatementRef},
    ColumnarBulkInserter, CursorImpl, Error, ParameterCollectionRef, ResultSetMetadata,
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
    pub fn into_statement(self) -> S {
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
        execute_with_parameters(move || Ok(stmt), None, params)
    }

    /// Describes parameter marker associated with a prepared SQL statement.
    ///
    /// # Parameters
    ///
    /// * `parameter_number`: Parameter marker number ordered sequentially in increasing parameter
    ///   order, starting at 1.
    pub fn describe_param(&mut self, parameter_number: u16) -> Result<ParameterDescription, Error> {
        let stmt = self.as_stmt_ref();

        stmt.describe_param(parameter_number).into_result(&stmt)
    }

    /// Number of placeholders which must be provided with [`Self::execute`] in order to execute
    /// this statement. This is equivalent to the number of placeholders used in the SQL string
    /// used to prepare the statement.
    pub fn num_params(&mut self) -> Result<u16, Error> {
        let stmt = self.as_stmt_ref();
        stmt.num_params().into_result(&stmt)
    }

    /// Number of placeholders which must be provided with [`Self::execute`] in order to execute
    /// this statement. This is equivalent to the number of placeholders used in the SQL string
    /// used to prepare the statement.
    ///
    /// ```
    /// use odbc_api::{Connection, Error, handles::ParameterDescription};
    ///
    /// fn collect_parameter_descriptions(
    ///     connection: Connection<'_>
    /// ) -> Result<Vec<ParameterDescription>, Error>{
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
        impl DoubleEndedIterator<Item = Result<ParameterDescription, Error>>
            + ExactSizeIterator<Item = Result<ParameterDescription, Error>>
            + '_,
        Error,
    > {
        Ok((1..=self.num_params()?).map(|index| self.describe_param(index)))
    }

    /// Unless you want to roll your own column buffer implementation users are encouraged to use
    /// [`Self::into_text_inserter`] instead.
    ///
    /// # Safety
    ///
    /// * Parameters must all be valid for insertion. An example for an invalid parameter would be
    ///   a text buffer with a cell those indiactor value exceeds the maximum element length. This
    ///   can happen after when truncation occurs then writing into a buffer.
    pub unsafe fn unchecked_bind_columnar_array_parameters<C>(
        self,
        parameter_buffers: Vec<C>,
    ) -> Result<ColumnarBulkInserter<S, C>, Error>
    where
        C: ColumnBuffer + HasDataType,
    {
        // We know that statement is a prepared statement.
        ColumnarBulkInserter::new(self.into_statement(), parameter_buffers)
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
        let parameter_buffers = max_str_len
            .map(|max_str_len| TextColumn::new(capacity, max_str_len))
            .collect();
        // Text Columns are created with NULL as default, which is valid for insertion.
        unsafe { self.unchecked_bind_columnar_array_parameters(parameter_buffers) }
    }

    /// A [`crate::ColumnarBulkInserter`] which takes ownership of both the statement and the bound
    /// array parameter buffers.
    ///
    /// ```no_run
    /// use odbc_api::{Connection, Error, IntoParameter, buffers::BufferDesc};
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
    ///     let buffer_description = [
    ///         BufferDesc::Text { max_str_len: 255 },
    ///         BufferDesc::I16 { nullable: false },
    ///     ];
    ///     // The capacity must be able to hold at least the largest batch. We do everything in one
    ///     // go, so we set it to the length of the input parameters.
    ///     let capacity = names.len();
    ///     // Allocate memory for the array column parameters and bind it to the statement.
    ///     let mut prebound = prepared.into_column_inserter(capacity, buffer_description)?;
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
        descriptions: impl IntoIterator<Item = BufferDesc>,
    ) -> Result<ColumnarBulkInserter<S, AnyBuffer>, Error> {
        let parameter_buffers = descriptions
            .into_iter()
            .map(|desc| AnyBuffer::from_desc(capacity, desc))
            .collect();
        unsafe { self.unchecked_bind_columnar_array_parameters(parameter_buffers) }
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
        descriptions: impl IntoIterator<Item = BufferDesc>,
    ) -> Result<ColumnarBulkInserter<StatementRef<'_>, AnyBuffer>, Error> {
        let stmt = self.statement.as_stmt_ref();

        let parameter_buffers = descriptions
            .into_iter()
            .map(|desc| AnyBuffer::from_desc(capacity, desc))
            .collect();
        unsafe { ColumnarBulkInserter::new(stmt, parameter_buffers) }
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
        let stmt = self.statement.as_stmt_ref();
        stmt.row_count().into_result(&stmt).map(|count| {
            // ODBC returns -1 in case a row count is not available
            if count == -1 {
                None
            } else {
                Some(count.try_into().unwrap())
            }
        })
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

use crate::{
    buffers::{AnyColumnBuffer, BufferDescription, ColumnBuffer, TextColumn},
    execute::execute_with_parameters,
    handles::{HasDataType, ParameterDescription, Statement, StatementImpl, StatementRef, AsStatementRef},
    prebound::PinnedParameterCollection,
    ColumnarBulkInserter, CursorImpl, Error, ParameterCollectionRef, Prebound, ResultSetMetadata,
};

/// A prepared query. Prepared queries are useful if the similar queries should executed more than
/// once.
pub struct Prepared<'open_connection> {
    statement: StatementImpl<'open_connection>,
}

impl<'o> Prepared<'o> {
    pub(crate) fn new(statement: StatementImpl<'o>) -> Self {
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
    pub fn into_statement(self) -> StatementImpl<'o> {
        self.statement
    }

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
    ) -> Result<Option<CursorImpl<&mut StatementImpl<'o>>>, Error> {
        execute_with_parameters(move || Ok(&mut self.statement), None, params)
    }

    /// Describes parameter marker associated with a prepared SQL statement.
    ///
    /// # Parameters
    ///
    /// * `parameter_number`: Parameter marker number ordered sequentially in increasing parameter
    ///   order, starting at 1.
    pub fn describe_param(&self, parameter_number: u16) -> Result<ParameterDescription, Error> {
        self.statement
            .describe_param(parameter_number)
            .into_result(&self.statement)
    }

    /// Bind parameter buffers to the statement. Your motivation for doing so would be that in order
    /// to execute the statement multiple times with different arguments it is now enough to modify
    /// the parameters in the buffer, rather than repeatedly binding new parameters to the
    /// statement. You now need fewer (potentially costly) odbc api calls for the same result.
    /// However in some situations (depending on the size of the paramteres) modifying the buffers
    /// and coping their contents might be more costly than rebinding to a different source. Also
    /// the requirements for these permantent buffers are higher, as they may not become invalid
    /// after the statment is executed, and if the [`Prebound`] instance is moved.
    ///
    /// ```
    /// use odbc_api::{Connection, Error, Prebound};
    /// use std::io::{self, stdin, Read};
    ///
    /// fn make_query<'a>(conn: &'a Connection<'_>) -> Result<Prebound<'a, Box<i32>>, Error>{
    ///     let mut query = "SELECT title FROM Movies WHERE year=?;";
    ///     let prepared = conn.prepare(query)?;
    ///     // We allocate the year parameter on the heap so it's not invalidated once we transfer
    ///     // ownership of the prepared statement + parameter to the caller of the function. Of
    ///     // course the compiler would catch it, if we missed this by mistake.
    ///     let year = Box::new(0);
    ///     let prebound = prepared.bind_parameters(year)?;
    ///     Ok(prebound)
    /// }
    ///
    /// // Later we may execute the query like this
    /// fn use_query(movies_by_year: &mut Prebound<'_, Box<i32>>) -> Result<(), Error> {
    ///     // Let's say we are interested in Movie titles released in 2021. Modify the parameter
    ///     // buffer accordingly.
    ///     *movies_by_year.params_mut() = 2021;
    ///     // and execute. Note that we do not specify the parameter here, since it is already
    ///     // bound.
    ///     let cursor = movies_by_year.execute()?;
    ///
    ///     // ... process cursor ...
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    pub fn bind_parameters<P>(self, parameters: P) -> Result<Prebound<'o, P>, Error>
    where
        P: PinnedParameterCollection,
    {
        // We know that statement is a prepared statement.
        unsafe { Prebound::new(self.into_statement(), parameters) }
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
    ) -> Result<ColumnarBulkInserter<'o, C>, Error>
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
    ///     // real usecase you are likely to achieve a better results with a higher batch size.
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
    ) -> Result<ColumnarBulkInserter<'o, TextColumn<u8>>, Error> {
        let max_str_len = max_str_len.into_iter();
        let parameter_buffers = max_str_len
            .map(|max_str_len| TextColumn::new(capacity, max_str_len))
            .collect();
        // Text Columns are created with NULL as default, which is valid for insertion.
        unsafe { self.unchecked_bind_columnar_array_parameters(parameter_buffers) }
    }

    pub fn into_any_column_inserter(
        self,
        capacity: usize,
        descriptions: impl IntoIterator<Item = BufferDescription>,
    ) -> Result<ColumnarBulkInserter<'o, AnyColumnBuffer>, Error> {
        let parameter_buffers = descriptions
            .into_iter()
            .map(|desc| AnyColumnBuffer::from_description(capacity, desc))
            .collect();
        unsafe { self.unchecked_bind_columnar_array_parameters(parameter_buffers) }
    }
}

impl<'o> ResultSetMetadata for Prepared<'o> {}

impl<'o> AsStatementRef for Prepared<'o> {
    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        self.statement.as_stmt_ref()
    }
}
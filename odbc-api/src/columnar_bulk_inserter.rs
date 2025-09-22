use std::cmp::min;

use crate::{
    CursorImpl, Error,
    buffers::{ColumnBuffer, Resize, TextColumn},
    execute::execute,
    handles::{AsStatementRef, HasDataType, Statement, StatementRef},
};

/// Can be used to execute a statement with bulk array parameters. Contrary to its name any statement
/// with parameters can be executed, not only `INSERT`; however, inserting large amounts of data in
/// batches is the primary intended use case.
///
/// Binding new buffers is quite expensive in ODBC, so the parameter buffers are reused for each
/// batch (so the pointers bound to the statement stay valid). So we copy each batch of data into the
/// buffers already bound first rather than binding user-defined buffers. Often the data might need
/// to be transformed anyway, so the copy is no actual overhead. Once the buffers are filled with a
/// batch, we send the data.
pub struct ColumnarBulkInserter<S, C> {
    // We maintain the invariant that the parameters are bound to the statement that parameter set
    // size reflects the number of valid rows in the batch.
    statement: S,
    /// The number of values within the buffers that should be inserted in the next roundtrip. The
    /// capacity of the buffers must be equal or larger than this value. The individual buffers
    /// just hold values. They have no concept of how much values they hold within their capacity.
    /// We maintain only this one value here for all the column buffers.
    parameter_set_size: usize,
    capacity: usize,
    /// We maintain the invariant that none of these buffers is truncated. I.e. indicator values in
    /// these buffers do not specify values that are larger than the maximum element length of the
    /// buffer. Some drivers have safeguards against this, but others would just take the length of
    /// the indicator value to read the value. This would mean reading "random" memory and
    /// interpreting it as the value. So the kind of thing we try to avoid in a safe abstraction.
    parameters: Vec<C>,
}

impl<S, C> ColumnarBulkInserter<S, C>
where
    S: AsStatementRef,
{
    /// Users are not encouraged to call this directly.
    ///
    /// # Safety
    ///
    /// * Statement is expected to be a perpared statement.
    /// * Parameters must all be valid for insertion. An example for an invalid parameter would be
    ///   a text buffer with a cell those indiactor value exceeds the maximum element length. This
    ///   can happen after when truncation occurs then writing into a buffer.
    pub unsafe fn new(
        mut statement: S,
        parameters: Vec<C>,
        mapping: impl InputParameterMapping,
    ) -> Result<Self, Error>
    where
        C: ColumnBuffer + HasDataType + Send,
    {
        let stmt = statement.as_stmt_ref();
        bind_parameter_buffers_to_statement(&parameters, mapping, stmt)?;
        let capacity = parameters
            .iter()
            .map(|col| col.capacity())
            .min()
            .unwrap_or(0);
        Ok(Self {
            statement,
            parameter_set_size: 0,
            capacity,
            parameters,
        })
    }

    /// Execute the prepared statement, with the parameters bound
    pub fn execute(&mut self) -> Result<Option<CursorImpl<StatementRef<'_>>>, Error> {
        let mut stmt = self.statement.as_stmt_ref();
        unsafe {
            if self.parameter_set_size == 0 {
                // A batch size of 0 will not execute anything, same as for execute on connection or
                // prepared.
                Ok(None)
            } else {
                // We reset the parameter set size, in order to adequatly handle batches of
                // different size then inserting into the database.
                stmt.set_paramset_size(self.parameter_set_size);
                execute(stmt, None)
            }
        }
    }

    /// Sets the number of rows in the buffer to zero.
    pub fn clear(&mut self) {
        self.parameter_set_size = 0;
    }

    /// Number of valid rows in the buffer
    pub fn num_rows(&self) -> usize {
        self.parameter_set_size
    }

    /// Set number of valid rows in the buffer. Must not be larger than the batch size. If the
    /// specified number than the number of valid rows currently held by the buffer additional they
    /// will just hold the value previously assigned to them. Therfore if extending the number of
    /// valid rows users should take care to assign values to these rows. However, even if not
    /// assigend it is always guaranteed that every cell is valid for insertion and will not cause
    /// out of bounds access down in the ODBC driver. Therefore this method is safe. You can set
    /// the number of valid rows before or after filling values into the buffer, but you must do so
    /// before executing the query.
    pub fn set_num_rows(&mut self, num_rows: usize) {
        if num_rows > self.capacity {
            panic!(
                "Columnar buffer may not be resized to a value higher than the maximum number of \
                rows initially specified in the constructor."
            );
        }
        self.parameter_set_size = num_rows;
    }

    /// Use this method to gain write access to the actual column data.
    ///
    /// # Parameters
    ///
    /// * `buffer_index`: Please note that the buffer index is not identical to the ODBC column
    ///   index. For one it is zero based. It also indexes the buffer bound, and not the columns of
    ///   the output result set. This is important, because not every column needs to be bound. Some
    ///   columns may simply be ignored. That being said, if every column of the output is bound in
    ///   the buffer, in the same order in which they are enumerated in the result set, the
    ///   relationship between column index and buffer index is `buffer_index = column_index - 1`.
    ///
    /// # Example
    ///
    /// This method is intended to be called if using [`ColumnarBulkInserter`] for column wise bulk
    /// inserts.
    ///
    /// ```no_run
    /// use odbc_api::{Connection, Error, buffers::BufferDesc};
    ///
    /// fn insert_birth_years(conn: &Connection, names: &[&str], years: &[i16])
    ///     -> Result<(), Error>
    /// {
    ///
    ///     // All columns must have equal length.
    ///     assert_eq!(names.len(), years.len());
    ///     // Prepare the insert statement
    ///     let prepared = conn.prepare("INSERT INTO Birthdays (name, year) VALUES (?, ?)")?;
    ///     // Create a columnar buffer which fits the input parameters.
    ///     let buffer_description = [
    ///         BufferDesc::Text { max_str_len: 255 },
    ///         BufferDesc::I16 { nullable: false },
    ///     ];
    ///     // Here we do everything in one batch. So the capacity is the number of input
    ///     // parameters.
    ///     let capacity = names.len();
    ///     let mut prebound = prepared.into_column_inserter(capacity, buffer_description)?;
    ///     // Set number of input rows in the current batch.
    ///     prebound.set_num_rows(names.len());
    ///     // Fill the buffer with values column by column
    ///
    ///     // Fill names
    ///     let mut col = prebound
    ///         .column_mut(0)
    ///         .as_text_view()
    ///         .expect("We know the name column to hold text.");
    ///     for (index, name) in names.iter().map(|s| Some(s.as_bytes())).enumerate() {
    ///         col.set_cell(index, name);
    ///     }
    ///
    ///     // Fill birth years
    ///     let mut col = prebound
    ///         .column_mut(1)
    ///         .as_slice::<i16>()
    ///         .expect("We know the year column to hold i16.");
    ///     col.copy_from_slice(years);
    ///
    ///     // Execute the prepared statment with the bound array parameters. Sending the values to
    ///     // the database.
    ///     prebound.execute()?;
    ///     Ok(())
    /// }
    /// ```
    pub fn column_mut<'a>(&'a mut self, buffer_index: usize) -> C::SliceMut
    where
        C: BoundInputSlice<'a>,
    {
        unsafe {
            self.parameters[buffer_index]
                .as_view_mut((buffer_index + 1) as u16, self.statement.as_stmt_ref())
        }
    }

    /// Maximum number of rows the buffer can hold at once.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Resize the buffers to the new capacity. It would be hard to maintain the invariants in case
    /// of an error, so this is why this method is destroying self in case something goes wrong.
    ///
    /// Valid rows in the buffer will be preserved. If the new capacity is smaller than the the
    /// parameter set size (the number of valid rows in the buffer), then these values will be
    /// dropped.
    ///
    /// You may want to make use of this method in case your program flow reads from a data source
    /// with varying batch sizes and you want to insert each batch in one roundtrip. If you do not
    /// know the maximum batch size in advance, you may need to resize the buffers on the fly.
    ///
    /// # Parameters
    ///
    /// * `new_capacity`: The new capacity of the buffers. Must be at least `1`. May be smaller or
    ///   larger than the current capacity.
    /// * `mapping`: The mapping of the input parameters to the column buffers. This should be equal
    ///   to the mapping used to create the `ColumnarBulkInserter` in the first place. If a mapping
    ///   has not been specified, explicitly you can use the [`InOrder`] mapping.
    pub fn resize(
        mut self,
        new_capacity: usize,
        mapping: impl InputParameterMapping,
    ) -> Result<Self, Error>
    where
        C: ColumnBuffer + HasDataType + Resize + Send,
    {
        assert!(new_capacity > 0, "New capacity must be at least 1.");
        // Resize the buffers
        for column in &mut self.parameters {
            column.resize(new_capacity);
        }
        self.capacity = new_capacity;
        self.parameter_set_size = min(self.parameter_set_size, new_capacity);
        // Now the pointers bound to the statement may be invalid, so we need to rebind them.
        let stmt = self.statement.as_stmt_ref();
        bind_parameter_buffers_to_statement(&self.parameters, mapping, stmt)?;
        Ok(self)
    }
}

/// Binds the column buffers to the statement, based on the mapping provided. It ensures that only
/// the provided parameter buffers are bound to the statement. Any previously bound buffers will be
/// no longer bound. In case of an error no buffers will be bound to the statement.
fn bind_parameter_buffers_to_statement<C>(
    parameters: &[C],
    mapping: impl InputParameterMapping,
    mut stmt: StatementRef<'_>,
) -> Result<(), Error>
where
    C: ColumnBuffer + HasDataType + Send,
{
    stmt.reset_parameters();
    let parameter_indices = 1..(mapping.num_parameters() as u16 + 1);
    for parameter_index in parameter_indices {
        let column_index = mapping.parameter_index_to_column_index(parameter_index);
        let column_buffer = &parameters[column_index];
        if let Err(error) =
            unsafe { stmt.bind_input_parameter(parameter_index, column_buffer) }.into_result(&stmt)
        {
            // This early return using `?` is risky. We actually did bind some parameters
            // already. We cannot guarantee that the bound pointers stay valid in case of an
            // error since `Self` is never constructed. We would away with this, if we took
            // ownership of the statement and it is destroyed should the constructor not
            // succeed. However columnar bulk inserter can also be instantiated with borrowed
            // statements. This is why we reset the parameters on error.
            stmt.reset_parameters();
            return Err(error);
        }
    }
    Ok(())
}

/// You can obtain a mutable slice of a column buffer which allows you to change its contents.
///
/// # Safety
///
/// * If any operations have been performed which would invalidate the pointers bound to the
///   statement, the slice must use the statement handle to rebind the column, at the end of its
///   lifetime (at the latest).
/// * All values must be complete. I.e. none of the values must be truncated.
pub unsafe trait BoundInputSlice<'a> {
    /// Intended to allow for modifying buffer contents, while leaving the bound parameter buffers
    /// valid.
    type SliceMut;

    /// Obtain a mutable view on a parameter buffer in order to change the parameter value(s)
    /// submitted when executing the statement.
    ///
    /// # Safety
    ///
    /// * The statement must be the statment the column buffer is bound to. The index must be the
    ///   parameter index it is bound at.
    /// * All values must be complete. I.e. none of the values must be truncated.
    unsafe fn as_view_mut(
        &'a mut self,
        parameter_index: u16,
        stmt: StatementRef<'a>,
    ) -> Self::SliceMut;
}

impl<S> ColumnarBulkInserter<S, TextColumn<u8>> {
    /// Takes one element from the iterator for each internal column buffer and appends it to the
    /// end of the buffer. Should a cell of the row be too large for the associated column buffer,
    /// the column buffer will be reallocated with `1.2` times its size, and rebound to the
    /// statement.
    ///
    /// This method panics if it is tried to insert elements beyond batch size. It will also panic
    /// if row does not contain at least one item for each internal column buffer.
    pub fn append<'b>(
        &mut self,
        mut row: impl Iterator<Item = Option<&'b [u8]>>,
    ) -> Result<(), Error>
    where
        S: AsStatementRef,
    {
        if self.capacity == self.parameter_set_size {
            panic!("Trying to insert elements into TextRowSet beyond batch size.")
        }

        let mut col_index = 1;
        for column in &mut self.parameters {
            let text = row.next().expect(
                "Row passed to TextRowSet::append must contain one element for each column.",
            );
            if let Some(text) = text {
                unsafe {
                    column
                        .as_view_mut(col_index, self.statement.as_stmt_ref())
                        .ensure_max_element_length(text.len(), self.parameter_set_size)?;
                }
                column.set_value(self.parameter_set_size, Some(text));
            } else {
                column.set_value(self.parameter_set_size, None);
            }
            col_index += 1;
        }

        self.parameter_set_size += 1;

        Ok(())
    }
}

/// Governs how indices of bound buffers map to the indices of the parameters. If the order of the
/// input buffers matches the order of the placeholders in the SQL statement, then you can just use
/// [`InOrder`] as the mapping.
///
/// Then using array input parameters to determine the values of placeholders in an SQL statement to
/// be executed the indices of the placeholders and the column buffers may differ. For starters the
/// column buffer indices are zero based, whereas the parameter indices are one based. On top of
/// that more complex mappings can emerge if the same input buffer should be reused to fill in for
/// multiple placeholders. In case the same value would appear in the query twice.
pub trait InputParameterMapping {
    fn parameter_index_to_column_index(&self, paramteter_index: u16) -> usize;
    fn num_parameters(&self) -> usize;
}

/// An implementation of [`InputParameterMapping`] that should be used if the order of the column
/// buffers for the array input parameters matches the order of the placeholders in the SQL
/// Statement.
pub struct InOrder {
    /// Corresponds to the number of placeholders in the SQL statement.
    number_of_parameters: usize,
}

impl InOrder {
    /// Creates a new [`InOrder`] mapping for the given number of parameters. The number of
    /// paremeters is the number of placeholders (`?`) in the SQL statement.
    pub fn new(number_of_parameters: usize) -> Self {
        Self {
            number_of_parameters,
        }
    }
}

impl InputParameterMapping for InOrder {
    fn parameter_index_to_column_index(&self, paramteter_index: u16) -> usize {
        debug_assert_ne!(0, paramteter_index, "Parameter index must be one based.");
        // Each parameter matches exactly one column. Also the column index is zero based, but
        // parameter indices are one based.
        (paramteter_index - 1) as usize
    }

    fn num_parameters(&self) -> usize {
        self.number_of_parameters
    }
}

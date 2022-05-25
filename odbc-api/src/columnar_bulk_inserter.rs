use crate::{
    buffers::{ColumnBuffer, TextColumn},
    execute::execute,
    handles::{HasDataType, Statement, StatementImpl},
    CursorImpl, Error,
};

/// Can be used to execute a statement with bulk array paramters. Contrary to its name any statement
/// with parameters can be executed, not only `INSERT` however inserting large amounts of data in
/// batches is the primary intended usecase.
///
/// Binding new buffers is quite expensive in ODBC, so the parameter buffers are reused for each
/// batch (so the pointers bound to the statment stay valid). So we copy each batch of data into the
/// buffers already bound first rather than binding user defined buffer. Often the data might need
/// to be transformed anyway, so the copy is no actual overhead. Once the buffers are filled with a
/// batch, we send the data.
pub struct ColumnarBulkInserter<'o, C> {
    // We maintain the invariant that the parameters are bound to the statement that parameter set
    // size reflects the number of valid rows in the batch.
    statement: StatementImpl<'o>,
    parameter_set_size: usize,
    capacity: usize,
    parameters: Vec<C>,
}

impl<'o, C> ColumnarBulkInserter<'o, C> {
    /// Users are not encouraged to call this directly.
    ///
    /// # Safety
    ///
    /// * Statement is expected to be a perpared statement.
    /// * Parameters must all be valid for insertion. An example for an invalid parameter would be
    ///   a text buffer with a cell those indiactor value exceeds the maximum element length. This
    ///   can happen after when truncation occurs then writing into a buffer.
    pub unsafe fn new(mut statement: StatementImpl<'o>, parameters: Vec<C>) -> Result<Self, Error>
    where
        C: ColumnBuffer + HasDataType,
    {
        let mut parameter_number = 1;
        // Bind buffers to statement.
        for column in &parameters {
            statement
                .bind_input_parameter(parameter_number, column)
                // This early return using `?` is risky. We actually did bind some parameters
                // already. They would be invalid and we can not guarantee their correctness.
                // However we get away with this, because we take ownership of the statement, and it
                // is destroyed should the constructor not succeed. However this logic would need to
                // be adapted if we ever borrow the statement instead.
                .into_result(&statement)?;
            parameter_number += 1;
        }
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
    pub fn execute(&mut self) -> Result<Option<CursorImpl<&mut StatementImpl<'o>>>, Error> {
        unsafe {
            if self.parameter_set_size == 0 {
                // A batch size of 0 will not execute anything, same as for execute on connection or
                // prepared.
                Ok(None)
            } else {
                // We reset the parameter set size, in order to adequatly handle batches of
                // different size then inserting into the database.
                self.statement.set_paramset_size(self.parameter_set_size);
                execute(&mut self.statement, None)
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

    /// Set number of valid rows in the buffer. Mustt not be larger than the batch size. If the
    /// specified number than the number of valid rows currently held by the buffer additional they
    /// will just hold the value previously assigned to them. Therfore if extending the number of
    /// valid rows users should take care to assign values to these rows. However, even if not
    /// assigend it is always guaranteed that every cell is valid for insertion and will not cause
    /// out of bounds access down in the ODBC driver. Therfore this method is safe.
    pub fn set_num_rows(&mut self, num_rows: usize) {
        if num_rows > self.capacity as usize {
            panic!(
                "Columnar buffer may not be resized to a value higher than the maximum number of \
                rows initially specified in the constructor."
            );
        }
        self.parameter_set_size = num_rows;
    }
}

impl<'o> ColumnarBulkInserter<'o, TextColumn<u8>> {
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
    ) -> Result<(), Error> {
        if self.capacity == self.parameter_set_size {
            panic!("Trying to insert elements into TextRowSet beyond batch size.")
        }

        let mut col_index = 1;
        for column in &mut self.parameters {
            let text = row.next().expect(
                "Row passed to TextRowSet::append must contain one element for each column.",
            );
            if let Some(text) = text {
                // Column buffer is not large enough to hold the element. We must allocate a larger
                // buffer in order to hold it. This invalidates the pointers previously bound to
                // the statement. So we rebind them.
                if text.len() > column.max_len() {
                    let new_max_str_len = (text.len() as f64 * 1.2) as usize;
                    column.resize_max_str(new_max_str_len, self.parameter_set_size);
                    unsafe {
                        self.statement
                            .bind_input_parameter(col_index, column)
                            .into_result(&self.statement)?
                    }
                }
                column.append(self.parameter_set_size, Some(text));
            } else {
                column.append(self.parameter_set_size, None);
            }
            col_index += 1;
        }

        self.parameter_set_size += 1;

        Ok(())
    }
}

use std::collections::HashSet;

use crate::{
    handles::{CDataMut, HasDataType, Statement},
    Cursor, Error, ParameterRefCollection, RowSetBuffer,
};

/// Projections for ColumnBuffers, allowing for reading writing data while bound as a rowset or
/// parameter buffer without invalidating invariants of the type.
///
/// Intended as part for the ColumnBuffer trait. Currently seperated to allow to compile without
/// GAT.
///
/// # Safety
///
/// View may not allow access to invalid rows.
pub unsafe trait ColumnProjections<'a> {
    /// Immutable view on the column data. Used in safe abstractions. User must not be able to
    /// access uninitialized or invalid memory of the buffer through this interface.
    type View;

    /// Used to gain access to the buffer, if bound as a parameter for inserting.
    type ViewMut;
}

impl<C: ColumnBuffer> ColumnarRowSet<C> {
    /// Create a new instance from columns with unique indicies. Capacity of the buffer will be the
    /// minimum capacity of the columns.
    pub fn new(columns: Vec<(u16, C)>) -> Self {
        // Assert capacity
        let capacity = columns
            .iter()
            .map(|(_, col)| col.capacity())
            .min()
            .unwrap_or(0);

        // Assert uniqueness of indices
        let mut indices = HashSet::new();
        if columns
            .iter()
            .any(move |&(col_index, _)| !indices.insert(col_index))
        {
            panic!("Column indices must be unique.")
        }

        unsafe { Self::new_unchecked(capacity, columns) }
    }

    /// # Safety
    ///
    /// * Indices must be unique
    /// * Columns all must have enough `capacity`.
    pub unsafe fn new_unchecked(capacity: usize, columns: Vec<(u16, C)>) -> Self {
        ColumnarRowSet {
            num_rows: Box::new(0),
            max_rows: capacity,
            columns,
        }
    }

    /// Number of valid rows in the buffer.
    pub fn num_rows(&self) -> usize {
        *self.num_rows
    }

    /// Use this method to gain read access to the actual column data.
    ///
    /// # Parameters
    ///
    /// * `buffer_index`: Please note that the buffer index is not identical to the ODBC column
    ///   index. For once it is zero based. It also indexes the buffer bound, and not the columns of
    ///   the output result set. This is important, because not every column needs to be bound. Some
    ///   columns may simply be ignored. That being said, if every column of the output is bound in
    ///   the buffer, in the same order in which they are enumerated in the result set, the
    ///   relationship between column index and buffer index is `buffer_index = column_index - 1`.
    pub fn column(&self, buffer_index: usize) -> <C as ColumnProjections<'_>>::View {
        unsafe { self.columns[buffer_index].1.view(*self.num_rows) }
    }

    /// Use this method to gain write access to the actual column data.
    ///
    /// # Parameters
    ///
    /// * `buffer_index`: Please note that the buffer index is not identical to the ODBC column
    ///   index. For once it is zero based. It also indexes the buffer bound, and not the columns of
    ///   the output result set. This is important, because not every column needs to be bound. Some
    ///   columns may simply be ignored. That being said, if every column of the output is bound in
    ///   the buffer, in the same order in which they are enumerated in the result set, the
    ///   relationship between column index and buffer index is `buffer_index = column_index - 1`.
    ///
    /// # Example
    ///
    /// This method is intend to be called if using [`ColumnarRowSet`] for column wise bulk inserts.
    ///
    /// ```no_run
    /// use odbc_api::{
    ///     Connection, Error, IntoParameter,
    ///     buffers::{BufferDescription, BufferKind, AnyColumnViewMut, buffer_from_description}
    /// };
    ///
    /// fn insert_birth_years(conn: &Connection, names: &[&str], years: &[i16])
    ///     -> Result<(), Error>
    /// {
    ///
    ///     // All columns must have equal length.
    ///     assert_eq!(names.len(), years.len());
    ///
    ///     // Create a columnar buffer which fits the input parameters.
    ///     let buffer_description = [
    ///         BufferDescription {
    ///             kind: BufferKind::Text { max_str_len: 255 },
    ///             nullable: false,
    ///         },
    ///         BufferDescription {
    ///             kind: BufferKind::I16,
    ///             nullable: false,
    ///         },
    ///     ];
    ///     let mut buffer = buffer_from_description(
    ///         names.len(),
    ///         buffer_description.iter().copied()
    ///     );
    ///
    ///     // Fill the buffer with values column by column
    ///     match buffer.column_mut(0) {
    ///         AnyColumnViewMut::Text(mut col) => {
    ///             col.write(names.iter().map(|s| Some(s.as_bytes())))
    ///         }
    ///         _ => panic!("We know the name column to hold text.")
    ///     }
    ///
    ///     match buffer.column_mut(1) {
    ///         AnyColumnViewMut::I16(mut col) => {
    ///             col.copy_from_slice(years)
    ///         }
    ///         _ => panic!("We know the year column to hold i16.")
    ///     }
    ///
    ///     conn.execute(
    ///         "INSERT INTO Birthdays (name, year) VALUES (?, ?)",
    ///         &buffer
    ///     )?;
    ///     Ok(())
    /// }
    /// ```
    pub fn column_mut(&mut self, buffer_index: usize) -> <C as ColumnProjections<'_>>::ViewMut {
        unsafe { self.columns[buffer_index].1.view_mut(*self.num_rows) }
    }

    /// Set number of valid rows in the buffer. May not be larger than the batch size. If the
    /// specified number should be larger than the number of valid rows currently held by the buffer
    /// additional rows with the default value are going to be created.
    pub fn set_num_rows(&mut self, num_rows: usize) {
        if num_rows > self.max_rows as usize {
            panic!(
                "Columnar buffer may not be resized to a value higher than the maximum number of \
                rows initially specified in the constructor."
            );
        }
        if *self.num_rows < num_rows {
            for (_col_index, ref mut column) in &mut self.columns {
                column.fill_default(*self.num_rows, num_rows)
            }
        }
        *self.num_rows = num_rows;
    }
}

unsafe impl<C> RowSetBuffer for ColumnarRowSet<C>
where
    C: ColumnBuffer,
{
    fn bind_type(&self) -> usize {
        0 // Specify columnar binding
    }

    fn row_array_size(&self) -> usize {
        self.max_rows
    }

    fn mut_num_fetch_rows(&mut self) -> &mut usize {
        self.num_rows.as_mut()
    }

    unsafe fn bind_to_cursor(&mut self, cursor: &mut impl Cursor) -> Result<(), Error> {
        for (col_number, column) in &mut self.columns {
            cursor
                .stmt_mut()
                .bind_col(*col_number, column)
                .into_result(cursor.stmt_mut())?;
        }
        Ok(())
    }
}

unsafe impl<C> ParameterRefCollection for &ColumnarRowSet<C>
where
    C: ColumnBuffer,
{
    fn parameter_set_size(&self) -> usize {
        *self.num_rows
    }

    unsafe fn bind_parameters_to(&mut self, stmt: &mut impl Statement) -> Result<(), Error> {
        for &(parameter_number, ref buffer) in &self.columns {
            stmt.bind_input_parameter(parameter_number, buffer)
                .into_result(stmt)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::buffers::buffer_from_description_and_indices;

    use super::super::{BufferDescription, BufferKind};

    #[test]
    #[should_panic(expected = "Column indices must be unique.")]
    fn assert_unique_column_indices() {
        let bd = BufferDescription {
            nullable: false,
            kind: BufferKind::I32,
        };
        buffer_from_description_and_indices(1, [(1, bd), (2, bd), (1, bd)].iter().cloned());
    }
}

/// A columnar buffer intended to be bound with [crate::Cursor::bind_buffer] in order to obtain
/// results from a cursor.
///
/// This buffer is designed to be versatile. It supports a wide variety of usage scenarios. It is
/// efficient in retrieving data, but expensive to allocate, as columns are allocated separately.
/// This is required in order to efficiently allow for rebinding columns, if this buffer is used to
/// provide array input parameters those maximum size is not known in advance.
///
/// Most applications should find the overhead negligible, especially if instances are reused.
pub struct ColumnarRowSet<C> {
    /// Use a box, so it is safe for a cursor to take ownership of this buffer.
    num_rows: Box<usize>,
    /// aka: batch size, row array size
    max_rows: usize,
    /// Column index and bound buffer
    columns: Vec<(u16, C)>,
}

/// A buffer able to be used together with [`ColumnBuffer`].
///
/// # Safety
///
/// Views must not allow access to unintialized / invalid rows.
pub unsafe trait ColumnBuffer:
    for<'a> ColumnProjections<'a> + CDataMut + HasDataType
{
    /// # Safety
    ///
    /// Underlying buffer may not know how many elements have been written to it by the last ODBC
    /// function call. So we tell it how many, and get a save to use view in Return. Specifying an
    /// erroneous value for `valid_rows`, may therfore result in the construced view giving us
    /// access to invalid rows in a safe abstraction, which of course would be a Bug.
    unsafe fn view(&self, valid_rows: usize) -> <Self as ColumnProjections<'_>>::View;

    /// # Safety
    ///
    /// `valid_rows` must be valid, otherwise the safe abstraction would provide access to invalid
    /// memory.
    unsafe fn view_mut(&mut self, valid_rows: usize) -> <Self as ColumnProjections<'_>>::ViewMut;

    /// Fills the column with the default representation of values, between `from` and `to` index.
    fn fill_default(&mut self, from: usize, to: usize);

    /// Current capacity of the column
    fn capacity(&self) -> usize;
}

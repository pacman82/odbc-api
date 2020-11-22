use super::TextColumn;
use crate::{Cursor, Error, ParameterCollection, RowSetBuffer};
use std::str::Utf8Error;

/// This row set binds a string buffer to each column, which is large enough to hold the maximum
/// length string representation for each element in the row set at once.
pub struct TextRowSet {
    // Current implementation is straight forward. We could consider allocating one block of memory
    // in allocation instead.
    batch_size: u32,
    /// A mutable pointer to num_rows_fetched is passed to the C-API. It is used to write back the
    /// number of fetched rows. `num_rows_fetched` is heap allocated, so the pointer is not
    /// invalidated, even if the `TextRowSet` instance is moved in memory.
    num_rows: Box<usize>,
    buffers: Vec<TextColumn>,
}

impl TextRowSet {
    /// Use `cursor` to query the display size for each column of the row set and allocates the
    /// buffers accordingly.
    pub fn for_cursor(batch_size: u32, cursor: &impl Cursor) -> Result<TextRowSet, Error> {
        let num_cols = cursor.num_result_cols()?;
        let buffers = (1..(num_cols + 1))
            .map(|col_index| {
                let max_str_len = cursor.col_display_size(col_index as u16)? as usize;
                Ok(TextColumn::new(batch_size as usize, max_str_len))
            })
            .collect::<Result<_, Error>>()?;
        Ok(TextRowSet {
            batch_size,
            num_rows: Box::new(0),
            buffers,
        })
    }

    /// Creates a text buffer large enough to hold `batch_size` rows with one column for each item
    /// `max_str_lengths` of respective size.
    pub fn new(batch_size: u32, max_str_lengths: impl Iterator<Item = usize>) -> Self {
        let buffers = max_str_lengths
            .map(|max_str_len| TextColumn::new(batch_size as usize, max_str_len))
            .collect();
        TextRowSet {
            batch_size,
            num_rows: Box::new(0),
            buffers,
        }
    }

    /// Access the element at the specified position in the row set.
    pub fn at(&self, col_index: usize, row_index: usize) -> Option<&[u8]> {
        assert!(row_index < *self.num_rows as usize);
        unsafe { self.buffers[col_index].value_at(row_index) }
    }

    /// Access the element at the specified position in the row set.
    pub fn at_as_str(&self, col_index: usize, row_index: usize) -> Result<Option<&str>, Utf8Error> {
        self.at(col_index, row_index)
            .map(std::str::from_utf8)
            .transpose()
    }

    /// Return the number of columns in the row set.
    pub fn num_cols(&self) -> usize {
        self.buffers.len()
    }

    /// Return the number of rows in the row set.
    pub fn num_rows(&self) -> usize {
        *self.num_rows as usize
    }

    /// Takes one element from the iterater for each internal column buffer and appends it to the
    /// end of the buffer. Should the buffer be not large enough to hold the element, it will be
    /// reallocated with `1.2` times its size.
    ///
    /// This method panics if it is tried to insert elements beyond batch size. It will also panic
    /// if row does not contain at least one item for each internal column buffer.
    pub fn append<'a>(&mut self, mut row: impl Iterator<Item = Option<&'a [u8]>>) {
        if self.batch_size == *self.num_rows as u32 {
            panic!("Trying to insert elements into TextRowSet beyond batch size.")
        }

        let index = *self.num_rows;
        for column in &mut self.buffers {
            let text = row.next().expect(
                "row passed to TextRowSet::append must contain one element for each column.",
            );
            column.append(index, text);
        }

        *self.num_rows += 1;
    }

    /// Sets the number of rows in the buffer to zero.
    pub fn clear(&mut self) {
        *self.num_rows = 0;
    }
}

unsafe impl RowSetBuffer for TextRowSet {
    fn bind_type(&self) -> u32 {
        0 // Specify column wise binding.
    }

    unsafe fn bind_to_cursor(&mut self, cursor: &mut impl Cursor) -> Result<(), Error> {
        cursor.set_row_array_size(self.batch_size)?;
        cursor.set_num_rows_fetched(&mut self.num_rows)?;
        for (index, column_buffer) in self.buffers.iter_mut().enumerate() {
            let column_number = (index + 1) as u16;
            cursor.bind_col(column_number, column_buffer)?;
        }
        Ok(())
    }
}

unsafe impl ParameterCollection for &TextRowSet {
    fn parameter_set_size(&self) -> u32 {
        *self.num_rows as u32
    }

    unsafe fn bind_parameters_to(&self, stmt: &mut crate::handles::Statement) -> Result<(), Error> {
        let mut parameter_number = 1;
        for column in &self.buffers {
            stmt.bind_input_parameter(parameter_number, column)?;
            parameter_number += 1;
        }
        Ok(())
    }
}

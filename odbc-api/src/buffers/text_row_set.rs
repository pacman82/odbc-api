use super::{ColumnBuffer, TextColumn};
use crate::{Cursor, Error, RowSetBuffer};
use odbc_sys::ULen;
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
    num_rows_fetched: Box<ULen>,
    buffers: Vec<TextColumn>,
}

impl TextRowSet {
    /// Use `cursor` to query the display size for each column of the row set and allocates the
    /// buffers accordingly.
    pub fn new(batch_size: u32, cursor: &impl Cursor) -> Result<TextRowSet, Error> {
        let num_cols = cursor.num_result_cols()?;
        let buffers = (1..(num_cols + 1))
            .map(|col_index| {
                let max_str_len = cursor.col_display_size(col_index as u16)? as usize;
                Ok(TextColumn::new(batch_size as usize, max_str_len))
            })
            .collect::<Result<_, Error>>()?;
        Ok(TextRowSet {
            batch_size,
            num_rows_fetched: Box::new(0),
            buffers,
        })
    }

    /// Access the element at the specified position in the row set.
    pub fn at(&self, col_index: usize, row_index: usize) -> Option<&[u8]> {
        assert!(row_index < *self.num_rows_fetched as usize);
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
        *self.num_rows_fetched as usize
    }
}

unsafe impl RowSetBuffer for TextRowSet {
    unsafe fn bind_to_cursor(&mut self, cursor: &mut impl Cursor) -> Result<(), Error> {
        cursor.set_row_bind_type(0)?;
        cursor.set_row_array_size(self.batch_size)?;
        cursor.set_num_rows_fetched(&mut self.num_rows_fetched)?;
        for (index, column_buffer) in self.buffers.iter_mut().enumerate() {
            let column_number = (index + 1) as u16;
            cursor.bind_col(column_number, column_buffer.bind_arguments())?;
        }
        Ok(())
    }
}

use crate::{
    handles::{CData, CDataMut, HasDataType},
    DataType,
};

use log::debug;
use odbc_sys::{CDataType, NULL_DATA};
use std::{cmp::min, convert::TryInto, ffi::c_void, ffi::CStr};

/// A buffer intended to be bound to a column of a cursor. Elements of the buffer will contain a
/// variable amount of characters up to a maximum string length. Since most SQL types have a string
/// representation this buffer can be bound to a column of almost any type, ODBC driver and driver
/// manager should take care of the conversion. Since elements of this type have variable length an
/// indicator buffer needs to be bound, whether the column is nullable or not, and therefore does
/// not matter for this buffer.
#[derive(Debug)]
pub struct TextColumn {
    /// Maximum text length without terminating zero.
    max_str_len: usize,
    values: Vec<u8>,
    /// Elements in this buffer are either `NULL_DATA` or hold the length of the element in value
    /// with the same index. Please note that this value may be larger than `max_str_len` if the
    /// text has been truncated.
    indicators: Vec<isize>,
}

impl TextColumn {
    /// This will allocate a value and indicator buffer for `batch_size` elements. Each value may
    /// have a maximum length of `max_str_len`. This implies that `max_str_len` is increased by
    /// one in order to make space for the null terminating zero at the end of strings.
    pub fn new(batch_size: usize, max_str_len: usize) -> Self {
        TextColumn {
            max_str_len,
            values: vec![0; (max_str_len + 1) * batch_size],
            indicators: vec![0; batch_size],
        }
    }

    /// Return the value for the given row index.
    ///
    /// # Safety
    ///
    /// The column buffer does not know how many elements were in the last row group, and therefore
    /// can not guarantee the accessed element to be valid and in a defined state. It also can not
    /// panic on accessing an undefined element. It will panic however if `row_index` is larger or
    /// equal to the maximum number of elements in the buffer.
    pub unsafe fn value_at(&self, row_index: usize) -> Option<&CStr> {
        let str_len = self.indicators[row_index];
        if str_len == NULL_DATA {
            None
        } else {
            let offset = row_index * (self.max_str_len + 1);
            let length = min(self.max_str_len, str_len as usize);
            Some(
                CStr::from_bytes_with_nul(&self.values[offset..offset + length + 1])
                    .expect("ODBC driver indicated must indicate string length correctly."),
            )
        }
    }

    /// Changes the maximum string length the buffer can hold. This operation is useful if you find
    /// an unexpected large input string during insertion.
    ///
    /// This is however costly, as not only does the new buffer have to be allocated, but all values
    /// have to copied from the old to the new buffer.
    ///
    /// This method could also be used to reduce the maximum string length, which would truncate
    /// strings in the process.
    ///
    /// # Parameters
    ///
    /// * `new_max_str_len`: New maximum string length without terminating zero.
    /// * `num_rows`: Number of valid rows currently stored in this buffer.
    pub fn rebind(&mut self, new_max_str_len: usize, num_rows: usize) {
        debug!(
            "Rebinding text column buffer with {} elements. Maximum string length {} => {}",
            num_rows, self.max_str_len, new_max_str_len
        );

        let batch_size = self.indicators.len();
        // Allocate a new buffer large enough to hold a batch of strings with maximum length.
        let mut new_values = vec![0u8; (new_max_str_len + 1) * batch_size];
        // Copy values from old to new buffer.
        let max_copy_length = min(self.max_str_len, new_max_str_len);
        for ((&indicator, old_value), new_value) in self
            .indicators
            .iter()
            .zip(self.values.chunks_exact_mut(self.max_str_len + 1))
            .zip(new_values.chunks_exact_mut(new_max_str_len + 1))
            .take(num_rows)
        {
            if indicator != NULL_DATA {
                let num_bytes_to_copy = min(indicator as usize, max_copy_length);
                new_value[..num_bytes_to_copy].copy_from_slice(&old_value[..num_bytes_to_copy]);
            }
        }
        self.values = new_values;
        self.max_str_len = new_max_str_len;
    }

    /// Appends a new element to the column buffer. Rebinds the buffer to increase maximum string
    /// length should text be to large.
    ///
    /// # Parameters
    ///
    /// * `index`: Zero based index of the new row position. Must be equal to the number of rows
    ///   currently in the buffer.
    /// * `text`: Text to store without terminating zero.
    pub fn append(&mut self, index: usize, text: Option<&[u8]>) {
        if let Some(text) = text {
            if text.len() > self.max_str_len {
                let new_max_str_len = (text.len() as f64 * 1.2) as usize;
                self.rebind(new_max_str_len, index)
            }

            let offset = index * (self.max_str_len + 1);
            self.values[offset..offset + text.len()].copy_from_slice(text);
            // Add terminating zero to string.
            self.values[offset + text.len()] = 0;
            // And of course set the indicator correctly.
            self.indicators[index] = text.len().try_into().unwrap();
        } else {
            self.indicators[index] = NULL_DATA;
        }
    }

    /// Iterator over the first `num_rows` values of a text column.
    ///
    /// # Safety
    ///
    /// Num rows may not exceed the actualy amount of valid num_rows filled be the ODBC API. The
    /// column buffer does not know how many elements were in the last row group, and therefore can
    /// not guarantee the accessed element to be valid and in a defined state. It also can not panic
    /// on accessing an undefined element. It will panic however if `row_index` is larger or equal
    /// to the maximum number of elements in the buffer.
    pub unsafe fn iter(&self, num_rows: usize) -> TextColumnIt {
        TextColumnIt {
            pos: 0,
            num_rows,
            col: &self,
        }
    }

    /// Sets the value of the buffer at index at Null or the specified binary Text. This method will
    /// panic on out of bounds index, or if input holds a text which is larger than the maximum
    /// allowed element length. `input` must be specified without the terminating zero.
    pub fn set_value<'b>(&mut self, index: usize, input: Option<&'b [u8]>) {
        if let Some(input) = input {
            self.indicators[index] = input.len().try_into().unwrap();
            if input.len() > self.max_str_len {
                panic!(
                    "Tried to insert a value into a text buffer which is larger than the \
                    maximum allowed string length for the buffer."
                );
            }
            let start = (self.max_str_len + 1) * index;
            let end = start + input.len();
            let buf = &mut self.values[start..end];
            buf.copy_from_slice(input);
            // Let's insert a terminating zero at the end to be on the safe side, in case the
            // ODBC driver would not care about the value in the index buffer and only look for the
            // terminating zero.
            self.values[end + 1] = 0;
        } else {
            self.indicators[index] = NULL_DATA;
        }
    }

    /// Fills the column with NULL, between From and To
    pub fn fill_null(&mut self, from: usize, to: usize) {
        for index in from..to {
            self.indicators[index] = NULL_DATA;
        }
    }

    /// A writer able to fill the first `n` elements of the buffer, from an iterator.
    pub fn writer_n(&mut self, n: usize) -> TextColumnWriter<'_> {
        TextColumnWriter {
            column: self,
            to: n,
        }
    }
}

/// Iterator over a text column. See [`TextColumn::iter`]
#[derive(Debug)]
pub struct TextColumnIt<'c> {
    pos: usize,
    num_rows: usize,
    col: &'c TextColumn,
}

impl<'c> Iterator for TextColumnIt<'c> {
    type Item = Option<&'c CStr>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == self.num_rows {
            None
        } else {
            let ret = unsafe { Some(self.col.value_at(self.pos)) };
            self.pos += 1;
            ret
        }
    }
}

/// Fills a text column buffer with elements from an Iterator.
#[derive(Debug)]
pub struct TextColumnWriter<'a> {
    column: &'a mut TextColumn,
    /// Upper limit, the text column writer will not write beyond this index.
    to: usize,
}

impl<'a> TextColumnWriter<'a> {
    /// Fill the text column with values by consuming the iterator and copying its items into the
    /// buffer. It will not extract more items from the iterator than the buffer may hold. Should
    /// some elements be longer than the maximum string length a rebind is triggered and the buffer
    /// is reallocated to make room for the longer elements.
    pub fn write<'b>(&mut self, it: impl Iterator<Item = Option<&'b [u8]>>) {
        for (index, item) in it.enumerate().take(self.to) {
            if let Some(item) = item {
                if item.len() > self.column.max_str_len {
                    self.rebind((item.len() as f64 * 1.2) as usize)
                }
            }
            self.column.set_value(index, item)
        }
    }

    /// Use this method to change the maximum string length (excluding terminating zero).
    ///
    /// # Parameters
    ///
    /// * `new_max_len`: New maximum length of values in the column. Existing values are truncated.
    ///   The length is exculding the terminating zero.
    ///
    pub fn rebind(&mut self, new_max_len: usize) {
        self.column.rebind(new_max_len, self.to)
    }
}

unsafe impl CData for TextColumn {
    fn cdata_type(&self) -> CDataType {
        CDataType::Char
    }

    fn indicator_ptr(&self) -> *const isize {
        self.indicators.as_ptr()
    }

    fn value_ptr(&self) -> *const c_void {
        self.values.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> isize {
        (self.max_str_len + 1).try_into().unwrap()
    }
}

unsafe impl HasDataType for TextColumn {
    fn data_type(&self) -> DataType {
        DataType::Varchar {
            length: self.max_str_len,
        }
    }
}

unsafe impl CDataMut for TextColumn {
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        self.indicators.as_mut_ptr()
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.values.as_mut_ptr() as *mut c_void
    }
}

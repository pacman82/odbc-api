use crate::{
    handles::{CData, CDataMut, HasDataType},
    DataType,
};

use odbc_sys::{CDataType, NULL_DATA};
use std::{cmp::min, convert::TryInto, ffi::c_void};

/// A buffer intended to be bound to a column of a cursor. Elements of the buffer will contain a
/// variable amount of bytes up to a maximum length. Since elements of this type have variable
/// length an additional indicator buffer is also maintained, whether the column is nullable or not.
/// Therfore this buffer type is used for variable sized binary data wether it is nullable or not.
#[derive(Debug)]
pub struct BinColumn {
    /// Maximum element length.
    max_len: usize,
    values: Vec<u8>,
    /// Elements in this buffer are either `NULL_DATA` or hold the length of the element in value
    /// with the same index. Please note that this value may be larger than `max_len` if the value
    /// has been truncated.
    indicators: Vec<isize>,
}

impl BinColumn {
    /// This will allocate a value and indicator buffer for `batch_size` elements. Each value may
    /// have a maximum length of `max_len`.
    pub fn new(batch_size: usize, max_len: usize) -> Self {
        BinColumn {
            max_len,
            values: vec![0; max_len * batch_size],
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
    pub unsafe fn value_at(&self, row_index: usize) -> Option<&[u8]> {
        let len = self.indicators[row_index];
        if len == NULL_DATA {
            None
        } else {
            let offset = row_index * self.max_len;
            // Indicator value might be larger than max_len.
            let length = min(self.max_len, len as usize);
            Some(&self.values[offset..offset + length])
        }
    }

    /// Changes the maximum element length the buffer can hold. This operation is useful if you find
    /// an unexpected large input during insertion. All values in the buffer will be set to NULL.
    ///
    /// # Parameters
    ///
    /// * `new_max_len`: New maximum string length without terminating zero.
    pub fn set_max_len(&mut self, new_max_len: usize) {
        let batch_size = self.indicators.len();
        // Allocate a new buffer large enough to hold a batch of strings with maximum length.
        let new_values = vec![0u8; new_max_len * batch_size];
        // Set all indicators to NULL
        self.fill_null(0, batch_size);
        self.values = new_values;
        self.max_len = new_max_len;
    }

    /// Iterator over the first `num_rows` values of a binary column.
    ///
    /// # Safety
    ///
    /// Num rows may not exceed the actualy amount of valid num_rows filled be the ODBC API. The
    /// column buffer does not know how many elements were in the last row group, and therefore can
    /// not guarantee the accessed element to be valid and in a defined state. It also can not panic
    /// on accessing an undefined element. It will panic however if `row_index` is larger or equal
    /// to the maximum number of elements in the buffer.
    pub unsafe fn iter(&self, num_rows: usize) -> BinColumnIt {
        BinColumnIt {
            pos: 0,
            num_rows,
            col: &self,
        }
    }

    /// Sets the value of the buffer at index to NULL or the specified bytes. This method will panic
    /// on out of bounds index, or if input holds a value which is longer than the maximum allowed
    /// element length.
    pub fn set_value(&mut self, index: usize, input: Option<&[u8]>) {
        if let Some(input) = input {
            self.indicators[index] = input.len().try_into().unwrap();
            if input.len() > self.max_len {
                panic!(
                    "Tried to insert a value into a binary buffer which is larger than the maximum \
                    allowed element length for the buffer."
                );
            }
            let start = self.max_len * index;
            let end = start + input.len();
            let buf = &mut self.values[start..end];
            buf.copy_from_slice(input);
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
    pub fn writer_n(&mut self, n: usize) -> BinColumnWriter<'_> {
        BinColumnWriter {
            column: self,
            to: n,
        }
    }
}

/// Iterator over a binary column. See [`crate::buffers::AnyColumnView`]
#[derive(Debug)]
pub struct BinColumnIt<'c> {
    pos: usize,
    num_rows: usize,
    col: &'c BinColumn,
}

impl<'c> Iterator for BinColumnIt<'c> {
    type Item = Option<&'c [u8]>;

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

/// Fills a binary column buffer with elements from an Iterator. See
/// [`crate::buffers::AnyColumnViewMut`]
#[derive(Debug)]
pub struct BinColumnWriter<'a> {
    column: &'a mut BinColumn,
    /// Upper limit, the binary column writer will not write beyond this index.
    to: usize,
}

impl<'a> BinColumnWriter<'a> {
    /// Fill the binary column with values by consuming the iterator and copying its items into the
    /// buffer. It will not extract more items from the iterator than the buffer may hold. This
    /// method panics if elements of the iterator are larger than the maximum element length of the
    /// buffer.
    pub fn write<'b>(&mut self, it: impl Iterator<Item = Option<&'b [u8]>>) {
        for (index, item) in it.enumerate().take(self.to) {
            self.column.set_value(index, item)
        }
    }

    /// Changes the maximum element length the buffer can hold. This operation is useful if you find
    /// an unexpected large input during insertion. All values in the buffer will be set to NULL.
    ///
    /// # Parameters
    ///
    /// * `new_max_len`: New maximum element length
    pub fn set_max_len(&mut self, new_max_len: usize) {
        self.column.set_max_len(new_max_len)
    }
}

unsafe impl CData for BinColumn {
    fn cdata_type(&self) -> CDataType {
        CDataType::Binary
    }

    fn indicator_ptr(&self) -> *const isize {
        self.indicators.as_ptr()
    }

    fn value_ptr(&self) -> *const c_void {
        self.values.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> isize {
        self.max_len.try_into().unwrap()
    }
}

unsafe impl HasDataType for BinColumn {
    fn data_type(&self) -> DataType {
        DataType::Varbinary {
            length: self.max_len,
        }
    }
}

unsafe impl CDataMut for BinColumn {
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        self.indicators.as_mut_ptr()
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.values.as_mut_ptr() as *mut c_void
    }
}

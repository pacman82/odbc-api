use crate::{
    buffers::Indicator,
    handles::{CData, CDataMut, HasDataType},
    DataType,
};

use log::debug;
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

    /// Maximum length of elements in bytes.
    pub fn max_len(&self) -> usize {
        self.max_len
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

    /// Changes the maximum number of bytes per row the buffer can hold. This operation is useful if
    /// you find an unexpected large input during insertion.
    ///
    /// This is however costly, as not only does the new buffer have to be allocated, but all values
    /// have to copied from the old to the new buffer.
    ///
    /// This method could also be used to reduce the maximum length, which would truncate values in
    /// the process.
    ///
    /// This method does not adjust indicator buffers as these might hold values larger than the
    /// maximum length.
    ///
    /// # Parameters
    ///
    /// * `new_max_len`: New maximum element length in bytes.
    /// * `num_rows`: Number of valid rows currently stored in this buffer.
    pub fn rebind(&mut self, new_max_len: usize, num_rows: usize) {
        debug!(
            "Rebinding binary column buffer with {} elements. Maximum length {} => {}",
            num_rows, self.max_len, new_max_len
        );

        let batch_size = self.indicators.len();
        // Allocate a new buffer large enough to hold a batch of elements with maximum length.
        let mut new_values = vec![0; new_max_len * batch_size];
        // Copy values from old to new buffer.
        let max_copy_length = min(self.max_len, new_max_len);
        for ((&indicator, old_value), new_value) in self
            .indicators
            .iter()
            .zip(self.values.chunks_exact_mut(self.max_len))
            .zip(new_values.chunks_exact_mut(new_max_len))
            .take(num_rows)
        {
            match Indicator::from_isize(indicator) {
                Indicator::Null => (),
                Indicator::NoTotal => {
                    // There is no good choice here in case we are expanding the buffer. Since
                    // NO_TOTAL indicates that we use the entire buffer, but in truth it would now
                    // be padded with 0. I currently cannot think of any usecase there it would
                    // matter.
                    new_value[..max_copy_length].clone_from_slice(&old_value[..max_copy_length]);
                }
                Indicator::Length(num_bytes_len) => {
                    let num_bytes_to_copy = min(num_bytes_len, max_copy_length);
                    new_value[..num_bytes_to_copy].copy_from_slice(&old_value[..num_bytes_to_copy]);
                }
            }
        }
        self.values = new_values;
        self.max_len = new_max_len;
    }

    /// Appends a new element to the column buffer. Rebinds the buffer to increase maximum element
    /// length should the input be to large.
    ///
    /// # Parameters
    ///
    /// * `index`: Zero based index of the new row position. Must be equal to the number of rows
    ///   currently in the buffer.
    /// * `bytes`: Value to store without terminating zero.
    pub fn append(&mut self, index: usize, bytes: Option<&[u8]>) {
        if let Some(text) = bytes {
            if text.len() > self.max_len {
                let new_max_str_len = (text.len() as f64 * 1.2) as usize;
                self.rebind(new_max_str_len, index)
            }

            let offset = index * self.max_len;
            self.values[offset..offset + text.len()].copy_from_slice(text);
            // Add terminating zero to string.
            self.values[offset + text.len()] = 0;
            // And of course set the indicator correctly.
            self.indicators[index] = text.len().try_into().unwrap();
        } else {
            self.indicators[index] = NULL_DATA;
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

    /// Maximum length
    pub fn max_len(&self) -> usize {
        self.column.max_len()
    }

    /// Changes the maximum element length the buffer can hold. This operation is useful if you find
    /// an unexpected large input during insertion.
    ///
    /// This is however costly, as not only does the new buffer have to be allocated, but all values
    /// have to copied from the old to the new buffer.
    ///
    /// This method could also be used to reduce the maximum element length, which would truncate
    /// values in the process.
    ///
    /// This method does not adjust indicator buffers as these might hold values larger than the
    /// maximum element length.
    ///
    /// # Parameters
    ///
    /// * `new_max_len`: New maximum element length.
    /// * `num_rows`: Number of valid rows currently stored in this buffer.
    pub fn rebind(&mut self, new_max_len: usize, num_rows: usize) {
        self.column.rebind(new_max_len, num_rows)
    }

    /// Inserts a new element to the column buffer. Rebinds the buffer to increase maximum element
    /// length should the value be larger than the maximum allowed element length. The number of
    /// rows the column buffer can hold stays constant, but during rebind only values befor `index`
    /// would be copied to the new memory location. Therefore this method is intended to be used to
    /// fill the buffer elementwise and in order. Hence the name `append`.
    ///
    /// # Parameters
    ///
    /// * `index`: Zero based index of the new row position. Must be equal to the number of rows
    ///   currently in the buffer.
    /// * `bytes`: Value to store
    ///
    /// # Example
    ///
    /// ```
    /// # use odbc_api::buffers::{
    ///     ColumnarRowSet, BufferDescription, BufferKind, AnyColumnViewMut, AnyColumnView
    /// };
    /// # use std::iter;
    ///
    /// let desc = BufferDescription {
    ///     // Buffer size purposefully chosen too small, so we need to increase the buffer size if we
    ///     // encounter larger inputs.
    ///     kind: BufferKind::Binary { length: 1 },
    ///     nullable: true,
    /// };
    ///
    /// // Input values to insert.
    /// let input : [Option<&[u8]>; 5]= [
    ///     Some(&[1]),
    ///     Some(&[2,3]),
    ///     Some(&[4,5,6]),
    ///     None,
    ///     Some(&[7,8,9,10,11,12]),
    /// ];
    ///
    /// let mut buffer = ColumnarRowSet::new(input.len() as u32, iter::once(desc));
    ///
    /// buffer.set_num_rows(input.len());
    /// if let AnyColumnViewMut::Binary(mut writer) = buffer.column_mut(0) {
    ///     for (index, &bytes) in input.iter().enumerate() {
    ///         writer.append(index, bytes)
    ///     }
    /// } else {
    ///     panic!("Expected binary column writer");
    /// }
    ///
    /// if let AnyColumnView::Binary(col) = buffer.column(0) {
    ///     assert!(col.zip(input.iter().copied()).all(|(expected, actual)| expected == actual))
    /// } else {
    ///     panic!("Expected binary column slice");   
    /// }
    /// ```
    ///
    pub fn append(&mut self, index: usize, text: Option<&[u8]>) {
        self.column.append(index, text)
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

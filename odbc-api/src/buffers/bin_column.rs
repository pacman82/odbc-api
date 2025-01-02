use crate::{
    buffers::Indicator,
    columnar_bulk_inserter::BoundInputSlice,
    error::TooLargeBufferSize,
    handles::{CData, CDataMut, HasDataType, Statement, StatementRef},
    DataType, Error,
};

use log::debug;
use odbc_sys::{CDataType, NULL_DATA};
use std::{cmp::min, ffi::c_void, num::NonZeroUsize};

/// A buffer intended to be bound to a column of a cursor. Elements of the buffer will contain a
/// variable amount of bytes up to a maximum length. Since elements of this type have variable
/// length an additional indicator buffer is also maintained, whether the column is nullable or not.
/// Therefore this buffer type is used for variable sized binary data whether it is nullable or not.
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
    /// have a maximum length of `element_size`. Uses a fallibale allocation for creating the
    /// buffer. In applications often the `element_size` of the buffer, might be directly inspired
    /// by the maximum size of the type, as reported, by ODBC. Which might get exceedingly large for
    /// types like VARBINARY(MAX), or IMAGE. On the downside, this method is potentially slower than
    /// new.
    pub fn try_new(batch_size: usize, element_size: usize) -> Result<Self, TooLargeBufferSize> {
        let len = element_size * batch_size;
        let mut values = Vec::new();
        values
            .try_reserve_exact(len)
            .map_err(|_| TooLargeBufferSize {
                num_elements: batch_size,
                element_size,
            })?;
        values.resize(len, 0);
        Ok(BinColumn {
            max_len: element_size,
            values,
            indicators: vec![0; batch_size],
        })
    }

    /// This will allocate a value and indicator buffer for `batch_size` elements. Each value may
    /// have a maximum length of `max_len`.
    pub fn new(batch_size: usize, element_size: usize) -> Self {
        let len = element_size * batch_size;
        let mut values = Vec::new();
        values.reserve_exact(len);
        values.resize(len, 0);
        BinColumn {
            max_len: element_size,
            values,
            indicators: vec![0; batch_size],
        }
    }

    /// Return the value for the given row index.
    ///
    /// The column buffer does not know how many elements were in the last row group, and therefore
    /// can not guarantee the accessed element to be valid and in a defined state. It also can not
    /// panic on accessing an undefined element. It will panic however if `row_index` is larger or
    /// equal to the maximum number of elements in the buffer.
    pub fn value_at(&self, row_index: usize) -> Option<&[u8]> {
        self.content_length_at(row_index).map(|length| {
            let offset = row_index * self.max_len;
            &self.values[offset..offset + length]
        })
    }

    /// Indicator value at the specified position. Useful to detect truncation of data.
    ///
    /// The column buffer does not know how many elements were in the last row group, and therefore
    /// can not guarantee the accessed element to be valid and in a defined state. It also can not
    /// panic on accessing an undefined element. It will panic however if `row_index` is larger or
    /// equal to the maximum number of elements in the buffer.
    pub fn indicator_at(&self, row_index: usize) -> Indicator {
        Indicator::from_isize(self.indicators[row_index])
    }

    /// Length of value at the specified position. This is different from an indicator as it refers
    /// to the length of the value in the buffer, not to the length of the value in the datasource.
    /// The two things are different for truncated values.
    pub fn content_length_at(&self, row_index: usize) -> Option<usize> {
        match self.indicator_at(row_index) {
            Indicator::Null => None,
            // Seen no total in the wild then binding shorter buffer to fixed sized CHAR in MSSQL.
            Indicator::NoTotal => Some(self.max_len),
            Indicator::Length(length) => {
                let length = min(self.max_len, length);
                Some(length)
            }
        }
    }

    /// `Some` if any value is truncated in the range [0, num_rows).
    ///
    /// After fetching data we may want to know if any value has been truncated due to the buffer
    /// not being able to hold elements of that size. This method checks the indicator buffer
    /// element wise and reports one indicator which indicates a size large than the maximum element
    /// size, if it exits.
    pub fn has_truncated_values(&self, num_rows: usize) -> Option<Indicator> {
        self.indicators
            .iter()
            .copied()
            .take(num_rows)
            .find_map(|indicator| {
                let indicator = Indicator::from_isize(indicator);
                indicator.is_truncated(self.max_len).then_some(indicator)
            })
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

    /// View of the first `num_rows` values of a binary column.
    ///
    /// Num rows may not exceed the actual amount of valid num_rows filled by the ODBC API. The
    /// column buffer does not know how many elements were in the last row group, and therefore can
    /// not guarantee the accessed element to be valid and in a defined state. It also can not panic
    /// on accessing an undefined element. It will panic however if `row_index` is larger or equal
    /// to the maximum number of elements in the buffer.
    pub fn view(&self, num_rows: usize) -> BinColumnView<'_> {
        BinColumnView {
            num_rows,
            col: self,
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
    pub fn resize_max_element_length(&mut self, new_max_len: usize, num_rows: usize) {
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
                    // be padded with 0. I currently cannot think of any use case there it would
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
    /// length should the input be too large.
    ///
    /// # Parameters
    ///
    /// * `index`: Zero based index of the new row position. Must be equal to the number of rows
    ///   currently in the buffer.
    /// * `bytes`: Value to store.
    pub fn append(&mut self, index: usize, bytes: Option<&[u8]>) {
        if let Some(bytes) = bytes {
            if bytes.len() > self.max_len {
                let new_max_len = (bytes.len() as f64 * 1.2) as usize;
                self.resize_max_element_length(new_max_len, index)
            }

            let offset = index * self.max_len;
            self.values[offset..offset + bytes.len()].copy_from_slice(bytes);
            // And of course set the indicator correctly.
            self.indicators[index] = bytes.len().try_into().unwrap();
        } else {
            self.indicators[index] = NULL_DATA;
        }
    }

    /// Maximum number of elements this buffer can hold.
    pub fn capacity(&self) -> usize {
        self.indicators.len()
    }
}

unsafe impl<'a> BoundInputSlice<'a> for BinColumn {
    type SliceMut = BinColumnSliceMut<'a>;

    unsafe fn as_view_mut(
        &'a mut self,
        parameter_index: u16,
        stmt: StatementRef<'a>,
    ) -> Self::SliceMut {
        BinColumnSliceMut {
            column: self,
            stmt,
            parameter_index,
        }
    }
}

/// A view to a mutable array parameter text buffer, which allows for filling the buffer with
/// values.
pub struct BinColumnSliceMut<'a> {
    column: &'a mut BinColumn,
    // Needed to rebind the column in case of reallocation
    stmt: StatementRef<'a>,
    // Also needed to rebind the column in case of reallocation
    parameter_index: u16,
}

impl BinColumnSliceMut<'_> {
    /// Sets the value of the buffer at index at Null or the specified binary Text. This method will
    /// panic on out of bounds index, or if input holds a text which is larger than the maximum
    /// allowed element length. `element` must be specified without the terminating zero.
    pub fn set_cell(&mut self, row_index: usize, element: Option<&[u8]>) {
        self.column.set_value(row_index, element)
    }

    /// Ensures that the buffer is large enough to hold elements of `element_length`. Does nothing
    /// if the buffer is already large enough. Otherwise it will reallocate and rebind the buffer.
    /// The first `num_rows_to_copy_elements` will be copied from the old value buffer to the new
    /// one. This makes this an extremly expensive operation.
    pub fn ensure_max_element_length(
        &mut self,
        element_length: usize,
        num_rows_to_copy: usize,
    ) -> Result<(), Error> {
        // Column buffer is not large enough to hold the element. We must allocate a larger buffer
        // in order to hold it. This invalidates the pointers previously bound to the statement. So
        // we rebind them.
        if element_length > self.column.max_len() {
            self.column
                .resize_max_element_length(element_length, num_rows_to_copy);
            unsafe {
                self.stmt
                    .bind_input_parameter(self.parameter_index, self.column)
                    .into_result(&self.stmt)?
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BinColumnView<'c> {
    num_rows: usize,
    col: &'c BinColumn,
}

impl<'c> BinColumnView<'c> {
    /// The number of valid elements in the text column.
    pub fn len(&self) -> usize {
        self.num_rows
    }

    /// True if, and only if there are no valid rows in the column buffer.
    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    /// Slice of text at the specified row index without terminating zero.
    pub fn get(&self, index: usize) -> Option<&'c [u8]> {
        self.col.value_at(index)
    }

    /// Iterator over the valid elements of the text buffer
    pub fn iter(&self) -> BinColumnIt<'c> {
        BinColumnIt {
            pos: 0,
            num_rows: self.num_rows,
            col: self.col,
        }
    }

    /// Finds an indicator larger than max element in the range [0, num_rows).
    ///
    /// After fetching data we may want to know if any value has been truncated due to the buffer
    /// not being able to hold elements of that size. This method checks the indicator buffer
    /// element wise.
    pub fn has_truncated_values(&self) -> Option<Indicator> {
        self.col.has_truncated_values(self.num_rows)
    }
}

/// Iterator over a binary column. See [`crate::buffers::BinColumn`]
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
            let ret = Some(self.col.value_at(self.pos));
            self.pos += 1;
            ret
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.num_rows - self.pos;
        (len, Some(len))
    }
}

impl ExactSizeIterator for BinColumnIt<'_> {}

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

impl HasDataType for BinColumn {
    fn data_type(&self) -> DataType {
        DataType::Varbinary {
            length: NonZeroUsize::new(self.max_len),
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

#[cfg(test)]
mod test {
    use crate::error::TooLargeBufferSize;

    use super::BinColumn;

    #[test]
    fn allocating_too_big_a_binary_column() {
        let two_gib = 2_147_483_648;
        let result = BinColumn::try_new(10_000, two_gib);
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            TooLargeBufferSize {
                num_elements: 10_000,
                element_size: 2_147_483_648
            }
        ))
    }
}

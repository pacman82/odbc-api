use crate::{
    fixed_sized::{Bit, Pod},
    handles::{CData, CDataMut},
};
use odbc_sys::{Date, Time, Timestamp, NULL_DATA};
use std::{
    ffi::c_void,
    mem::size_of,
    ptr::{null, null_mut},
};

pub type OptF64Column = ColumnWithIndicator<f64>;
pub type OptF32Column = ColumnWithIndicator<f32>;
pub type OptDateColumn = ColumnWithIndicator<Date>;
pub type OptTimestampColumn = ColumnWithIndicator<Timestamp>;
pub type OptTimeColumn = ColumnWithIndicator<Time>;
pub type OptI8Column = ColumnWithIndicator<i8>;
pub type OptI16Column = ColumnWithIndicator<i16>;
pub type OptI32Column = ColumnWithIndicator<i32>;
pub type OptI64Column = ColumnWithIndicator<i64>;
pub type OptU8Column = ColumnWithIndicator<u8>;
pub type OptBitColumn = ColumnWithIndicator<Bit>;

/// Column buffer for fixed sized type, also binding an indicator buffer to handle NULL.
#[derive(Debug)]
pub struct ColumnWithIndicator<T> {
    values: Vec<T>,
    indicators: Vec<isize>,
}

impl<T> ColumnWithIndicator<T>
where
    T: Default + Clone,
{
    pub fn new(batch_size: usize) -> Self {
        Self {
            values: vec![T::default(); batch_size],
            indicators: vec![NULL_DATA; batch_size],
        }
    }

    /// Access the value at a specific row index.
    ///
    /// The buffer size is not automatically adjusted to the size of the last row set. It is the
    /// callers responsibility to ensure, a value has been written to the indexed position by
    /// [`crate::Cursor::fetch`] using the value bound to the cursor with
    /// [`crate::Cursor::set_num_result_rows_fetched`].
    pub fn iter(&self, num_rows: usize) -> NullableSlice<'_, T> {
        NullableSlice {
            indicators: &self.indicators[0..num_rows],
            values: &self.values[0..num_rows],
        }
    }

    /// Fills the column with NULL, between From and To
    pub fn fill_null(&mut self, from: usize, to: usize) {
        for index in from..to {
            self.indicators[index] = NULL_DATA;
        }
    }

    /// Create a writer which writes to the first `n` elements of the buffer.
    pub fn writer_n(&mut self, n: usize) -> NullableSliceMut<'_, T> {
        NullableSliceMut {
            indicators: &mut self.indicators[0..n],
            values: &mut self.values[0..n],
        }
    }

    /// Maximum number elements which the column may hold.
    pub fn capacity(&self) -> usize {
        self.indicators.len()
    }
}

/// Iterates over the elements of a column buffer. Returned by
/// [`crate::buffers::ColumnarBuffer::column`] as part of an [`crate::buffers::AnySlice`].
#[derive(Debug, Clone, Copy)]
pub struct NullableSlice<'a, T> {
    indicators: &'a [isize],
    values: &'a [T],
}

impl<'a, T> NullableSlice<'a, T> {
    /// `true` if the slice has a length of `0`.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Number of entries in this slice of the buffer
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Read access to the underlying raw value and indicator buffer.
    ///
    /// The number of elements in the buffer is equal to the number of rows returned in the current
    /// result set. Yet the content of any value, those associated value in the indicator buffer is
    /// [`crate::sys::NULL_DATA`] is undefined.
    ///
    /// This method is useful for writing performant bindings to datastructures with similar binary
    /// layout, as it allows for using memcopy rather than iterating over individual values.
    ///
    /// # Example
    ///
    /// ```
    /// use odbc_api::{buffers::NullableSlice, sys::NULL_DATA};
    ///
    /// // Memcopy the values out of the buffer, and make a mask of bools indicating the NULL
    /// // values.
    /// fn copy_values_and_make_mask(odbc_slice: NullableSlice<i32>) -> (Vec<i32>, Vec<bool>) {
    ///     let (values, indicators) = odbc_slice.raw_values();
    ///     let values = values.to_vec();
    ///     // Create array of bools indicating null values.
    ///     let mask: Vec<bool> = indicators
    ///         .iter()
    ///         .map(|&indicator| indicator != NULL_DATA)
    ///         .collect();
    ///     (values, mask)
    /// }
    /// ```
    pub fn raw_values(&self) -> (&'a [T], &'a [isize]) {
        (self.values, self.indicators)
    }
}

impl<'a, T> Iterator for NullableSlice<'a, T> {
    type Item = Option<&'a T>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(&ind) = self.indicators.first() {
            let item = if ind == NULL_DATA {
                None
            } else {
                Some(&self.values[0])
            };
            self.indicators = &self.indicators[1..];
            self.values = &self.values[1..];
            Some(item)
        } else {
            None
        }
    }
}

unsafe impl<T> CData for ColumnWithIndicator<T>
where
    T: Pod,
{
    fn cdata_type(&self) -> odbc_sys::CDataType {
        T::C_DATA_TYPE
    }

    fn indicator_ptr(&self) -> *const isize {
        self.indicators.as_ptr()
    }

    fn value_ptr(&self) -> *const c_void {
        self.values.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> isize {
        size_of::<T>().try_into().unwrap()
    }
}

unsafe impl<T> CDataMut for ColumnWithIndicator<T>
where
    T: Pod,
{
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        self.indicators.as_mut_ptr()
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.values.as_mut_ptr() as *mut c_void
    }
}

unsafe impl<T> CData for Vec<T>
where
    T: Pod,
{
    fn cdata_type(&self) -> odbc_sys::CDataType {
        T::C_DATA_TYPE
    }

    fn indicator_ptr(&self) -> *const isize {
        null()
    }

    fn value_ptr(&self) -> *const c_void {
        self.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> isize {
        size_of::<T>().try_into().unwrap()
    }
}

unsafe impl<T> CDataMut for Vec<T>
where
    T: Pod,
{
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        null_mut()
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.as_mut_ptr() as *mut c_void
    }
}

/// Used to fill a column buffer with an iterator. Returned by
/// [`crate::ColumnarBulkInserter::column_mut`] as part of an [`crate::buffers::AnySliceMut`].
#[derive(Debug)]
pub struct NullableSliceMut<'a, T> {
    indicators: &'a mut [isize],
    values: &'a mut [T],
}

impl<T> NullableSliceMut<'_, T> {
    /// `true` if the slice has a length of `0`.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Number of entries in this slice of the buffer
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Sets the value at the specified index. Use `None` to specify a `NULL` value.
    pub fn set_cell(&mut self, index: usize, cell: Option<T>) {
        if let Some(value) = cell {
            self.indicators[index] = 0;
            self.values[index] = value;
        } else {
            self.indicators[index] = NULL_DATA;
        }
    }

    /// Write access to the underlying raw value and indicator buffer.
    ///
    /// The number of elements in the buffer is equal to `len`.
    ///
    /// This method is useful for writing performant bindings to datastructures with similar binary
    /// layout, as it allows for using memcopy rather than iterating over individual values.
    ///
    /// # Example
    ///
    /// ```
    /// use odbc_api::{buffers::NullableSliceMut, sys::NULL_DATA};
    ///
    /// // Memcopy the values into the buffer, and set indicators according to mask
    /// // values.
    /// fn copy_values_and_make_mask(
    ///     new_values: &[i32],
    ///     mask: &[bool],
    ///     odbc_slice: &mut NullableSliceMut<i32>)
    /// {
    ///     let (values, indicators) = odbc_slice.raw_values();
    ///     values.copy_from_slice(new_values);
    ///     // Create array of bools indicating null values.
    ///     indicators.iter_mut().zip(mask.iter()).for_each(|(indicator, &mask)| {
    ///         *indicator = if mask {
    ///             0
    ///         } else {
    ///             NULL_DATA
    ///         }
    ///     });
    /// }
    /// ```
    pub fn raw_values(&mut self) -> (&mut [T], &mut [isize]) {
        (self.values, self.indicators)
    }
}

impl<T> NullableSliceMut<'_, T> {
    /// Writes the elements returned by the iterator into the buffer, starting at the beginning.
    /// Writes elements until the iterator returns `None` or the buffer can not hold more elements.
    pub fn write(&mut self, it: impl Iterator<Item = Option<T>>) {
        for (index, item) in it.enumerate().take(self.values.len()) {
            self.set_cell(index, item)
        }
    }
}

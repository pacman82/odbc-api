use crate::{
    columnar_bulk_inserter::BoundInputSlice,
    error::TooLargeBufferSize,
    handles::{CData, CDataMut, HasDataType, Statement, StatementRef},
    DataType, Error,
};

use super::{ColumnBuffer, Indicator};

use log::debug;
use odbc_sys::{CDataType, NULL_DATA};
use std::{cmp::min, ffi::c_void, mem::size_of, num::NonZeroUsize, panic};
use widestring::U16Str;

/// A column buffer for character data. The actual encoding used may depend on your system locale.
pub type CharColumn = TextColumn<u8>;

/// This buffer uses wide characters which implies UTF-16 encoding. UTF-8 encoding is preferable for
/// most applications, but contrary to its sibling [`crate::buffers::CharColumn`] this buffer types
/// implied encoding does not depend on the system locale.
pub type WCharColumn = TextColumn<u16>;

/// A buffer intended to be bound to a column of a cursor. Elements of the buffer will contain a
/// variable amount of characters up to a maximum string length. Since most SQL types have a string
/// representation this buffer can be bound to a column of almost any type, ODBC driver and driver
/// manager should take care of the conversion. Since elements of this type have variable length an
/// indicator buffer needs to be bound, whether the column is nullable or not, and therefore does
/// not matter for this buffer.
///
/// Character type `C` is intended to be either `u8` or `u16`.
#[derive(Debug)]
pub struct TextColumn<C> {
    /// Maximum text length without terminating zero.
    max_str_len: usize,
    values: Vec<C>,
    /// Elements in this buffer are either `NULL_DATA` or hold the length of the element in value
    /// with the same index. Please note that this value may be larger than `max_str_len` if the
    /// text has been truncated.
    indicators: Vec<isize>,
}

impl<C> TextColumn<C> {
    /// This will allocate a value and indicator buffer for `batch_size` elements. Each value may
    /// have a maximum length of `max_str_len`. This implies that `max_str_len` is increased by
    /// one in order to make space for the null terminating zero at the end of strings. Uses a
    /// fallible allocation for creating the buffer. In applications often the `max_str_len` size
    /// of the buffer, might be directly inspired by the maximum size of the type, as reported, by
    /// ODBC. Which might get exceedingly large for types like VARCHAR(MAX)
    pub fn try_new(batch_size: usize, max_str_len: usize) -> Result<Self, TooLargeBufferSize>
    where
        C: Default + Copy,
    {
        // Element size is +1 to account for terminating zero
        let element_size = max_str_len + 1;
        let len = element_size * batch_size;
        let mut values = Vec::new();
        values
            .try_reserve_exact(len)
            .map_err(|_| TooLargeBufferSize {
                num_elements: batch_size,
                // We want the element size in bytes
                element_size: element_size * size_of::<C>(),
            })?;
        values.resize(len, C::default());
        Ok(TextColumn {
            max_str_len,
            values,
            indicators: vec![0; batch_size],
        })
    }

    /// This will allocate a value and indicator buffer for `batch_size` elements. Each value may
    /// have a maximum length of `max_str_len`. This implies that `max_str_len` is increased by
    /// one in order to make space for the null terminating zero at the end of strings. All
    /// indicators are set to [`crate::sys::NULL_DATA`] by default.
    pub fn new(batch_size: usize, max_str_len: usize) -> Self
    where
        C: Default + Copy,
    {
        // Element size is +1 to account for terminating zero
        let element_size = max_str_len + 1;
        let len = element_size * batch_size;
        let mut values = Vec::new();
        values.reserve_exact(len);
        values.resize(len, C::default());
        TextColumn {
            max_str_len,
            values,
            indicators: vec![NULL_DATA; batch_size],
        }
    }

    /// Bytes of string at the specified position. Includes interior nuls, but excludes the
    /// terminating nul.
    ///
    /// The column buffer does not know how many elements were in the last row group, and therefore
    /// can not guarantee the accessed element to be valid and in a defined state. It also can not
    /// panic on accessing an undefined element. It will panic however if `row_index` is larger or
    /// equal to the maximum number of elements in the buffer.
    pub fn value_at(&self, row_index: usize) -> Option<&[C]> {
        self.content_length_at(row_index).map(|length| {
            let offset = row_index * (self.max_str_len + 1);
            &self.values[offset..offset + length]
        })
    }

    /// Maximum length of elements
    pub fn max_len(&self) -> usize {
        self.max_str_len
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
            Indicator::NoTotal => Some(self.max_str_len),
            Indicator::Length(length_in_bytes) => {
                let length_in_chars = length_in_bytes / size_of::<C>();
                let length = min(self.max_str_len, length_in_chars);
                Some(length)
            }
        }
    }

    /// Finds an indiactor larger than the maximum element size in the range [0, num_rows).
    ///
    /// After fetching data we may want to know if any value has been truncated due to the buffer
    /// not being able to hold elements of that size. This method checks the indicator buffer
    /// element wise.
    pub fn has_truncated_values(&self, num_rows: usize) -> Option<Indicator> {
        let max_bin_length = self.max_str_len * size_of::<C>();
        self.indicators
            .iter()
            .copied()
            .take(num_rows)
            .find_map(|indicator| {
                let indicator = Indicator::from_isize(indicator);
                indicator.is_truncated(max_bin_length).then_some(indicator)
            })
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
    /// This method does not adjust indicator buffers as these might hold values larger than the
    /// maximum string length.
    ///
    /// # Parameters
    ///
    /// * `new_max_str_len`: New maximum string length without terminating zero.
    /// * `num_rows`: Number of valid rows currently stored in this buffer.
    pub fn resize_max_str(&mut self, new_max_str_len: usize, num_rows: usize)
    where
        C: Default + Copy,
    {
        debug!(
            "Rebinding text column buffer with {} elements. Maximum string length {} => {}",
            num_rows, self.max_str_len, new_max_str_len
        );

        let batch_size = self.indicators.len();
        // Allocate a new buffer large enough to hold a batch of strings with maximum length.
        let mut new_values = vec![C::default(); (new_max_str_len + 1) * batch_size];
        // Copy values from old to new buffer.
        let max_copy_length = min(self.max_str_len, new_max_str_len);
        for ((&indicator, old_value), new_value) in self
            .indicators
            .iter()
            .zip(self.values.chunks_exact_mut(self.max_str_len + 1))
            .zip(new_values.chunks_exact_mut(new_max_str_len + 1))
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
                    let num_bytes_to_copy = min(num_bytes_len / size_of::<C>(), max_copy_length);
                    new_value[..num_bytes_to_copy].copy_from_slice(&old_value[..num_bytes_to_copy]);
                }
            }
        }
        self.values = new_values;
        self.max_str_len = new_max_str_len;
    }

    /// Sets the value of the buffer at index at Null or the specified binary Text. This method will
    /// panic on out of bounds index, or if input holds a text which is larger than the maximum
    /// allowed element length. `input` must be specified without the terminating zero.
    pub fn set_value(&mut self, index: usize, input: Option<&[C]>)
    where
        C: Default + Copy,
    {
        if let Some(input) = input {
            self.set_mut(index, input.len()).copy_from_slice(input);
        } else {
            self.indicators[index] = NULL_DATA;
        }
    }

    /// Can be used to set a value at a specific row index without performing a memcopy on an input
    /// slice and instead provides direct access to the underlying buffer.
    ///
    /// In situations there the memcopy can not be avoided anyway [`Self::set_value`] is likely to
    /// be more convenient. This method is very useful if you want to `write!` a string value to the
    /// buffer and the binary (**!**) length of the formatted string is known upfront.
    ///
    /// # Example: Write timestamp to text column.
    ///
    /// ```
    /// use odbc_api::buffers::TextColumn;
    /// use std::io::Write;
    ///
    /// /// Writes times formatted as hh::mm::ss.fff
    /// fn write_time(
    ///     col: &mut TextColumn<u8>,
    ///     index: usize,
    ///     hours: u8,
    ///     minutes: u8,
    ///     seconds: u8,
    ///     milliseconds: u16)
    /// {
    ///     write!(
    ///         col.set_mut(index, 12),
    ///         "{:02}:{:02}:{:02}.{:03}",
    ///         hours, minutes, seconds, milliseconds
    ///     ).unwrap();
    /// }
    /// ```
    pub fn set_mut(&mut self, index: usize, length: usize) -> &mut [C]
    where
        C: Default,
    {
        if length > self.max_str_len {
            panic!(
                "Tried to insert a value into a text buffer which is larger than the maximum \
                allowed string length for the buffer."
            );
        }
        self.indicators[index] = (length * size_of::<C>()).try_into().unwrap();
        let start = (self.max_str_len + 1) * index;
        let end = start + length;
        // Let's insert a terminating zero at the end to be on the safe side, in case the ODBC
        // driver would not care about the value in the index buffer and only look for the
        // terminating zero.
        self.values[end] = C::default();
        &mut self.values[start..end]
    }

    /// Fills the column with NULL, between From and To
    pub fn fill_null(&mut self, from: usize, to: usize) {
        for index in from..to {
            self.indicators[index] = NULL_DATA;
        }
    }

    /// Provides access to the raw underlying value buffer. Normal applications should have little
    /// reason to call this method. Yet it may be useful for writing bindings which copy directly
    /// from the ODBC in memory representation into other kinds of buffers.
    ///
    /// The buffer contains the bytes for every non null valid element, padded to the maximum string
    /// length. The content of the padding bytes is undefined. Usually ODBC drivers write a
    /// terminating zero at the end of each string. For the actual value length call
    /// [`Self::content_length_at`]. Any element starts at index * ([`Self::max_len`] + 1).
    pub fn raw_value_buffer(&self, num_valid_rows: usize) -> &[C] {
        &self.values[..(self.max_str_len + 1) * num_valid_rows]
    }

    /// The maximum number of rows the TextColumn can hold.
    pub fn row_capacity(&self) -> usize {
        self.values.len()
    }
}

impl WCharColumn {
    /// The string slice at the specified position as `U16Str`. Includes interior nuls, but excludes
    /// the terminating nul.
    ///
    /// # Safety
    ///
    /// The column buffer does not know how many elements were in the last row group, and therefore
    /// can not guarantee the accessed element to be valid and in a defined state. It also can not
    /// panic on accessing an undefined element. It will panic however if `row_index` is larger or
    /// equal to the maximum number of elements in the buffer.
    pub unsafe fn ustr_at(&self, row_index: usize) -> Option<&U16Str> {
        self.value_at(row_index).map(U16Str::from_slice)
    }
}

unsafe impl<C: 'static> ColumnBuffer for TextColumn<C>
where
    TextColumn<C>: CDataMut + HasDataType,
{
    type View<'a> = TextColumnView<'a, C>;

    fn view(&self, valid_rows: usize) -> TextColumnView<'_, C> {
        TextColumnView {
            num_rows: valid_rows,
            col: self,
        }
    }

    fn fill_default(&mut self, from: usize, to: usize) {
        self.fill_null(from, to)
    }

    /// Maximum number of text strings this column may hold.
    fn capacity(&self) -> usize {
        self.indicators.len()
    }

    fn has_truncated_values(&self, num_rows: usize) -> Option<Indicator> {
        let max_bin_length = self.max_str_len * size_of::<C>();
        self.indicators
            .iter()
            .copied()
            .take(num_rows)
            .find_map(|indicator| {
                let indicator = Indicator::from_isize(indicator);
                indicator.is_truncated(max_bin_length).then_some(indicator)
            })
    }
}

/// Allows read only access to the valid part of a text column.
///
/// You may ask, why is this type required, should we not just be able to use `&TextColumn`? The
/// problem with `TextColumn` is, that it is a buffer, but it has no idea how many of its members
/// are actually valid, and have been returned with the last row group of the the result set. That
/// number is maintained on the level of the entire column buffer. So a text column knows the number
/// of valid rows, in addition to holding a reference to the buffer, in order to guarantee, that
/// every element acccessed through it, is valid.
#[derive(Debug, Clone, Copy)]
pub struct TextColumnView<'c, C> {
    num_rows: usize,
    col: &'c TextColumn<C>,
}

impl<'c, C> TextColumnView<'c, C> {
    /// The number of valid elements in the text column.
    pub fn len(&self) -> usize {
        self.num_rows
    }

    /// True if, and only if there are no valid rows in the column buffer.
    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    /// Slice of text at the specified row index without terminating zero.
    pub fn get(&self, index: usize) -> Option<&'c [C]> {
        self.col.value_at(index)
    }

    /// Iterator over the valid elements of the text buffer
    pub fn iter(&self) -> TextColumnIt<'c, C> {
        TextColumnIt {
            pos: 0,
            num_rows: self.num_rows,
            col: self.col,
        }
    }

    /// Length of value at the specified position. This is different from an indicator as it refers
    /// to the length of the value in the buffer, not to the length of the value in the datasource.
    /// The two things are different for truncated values.
    pub fn content_length_at(&self, row_index: usize) -> Option<usize> {
        if row_index >= self.num_rows {
            panic!("Row index points beyond the range of valid values.")
        }
        self.col.content_length_at(row_index)
    }

    /// Provides access to the raw underlying value buffer. Normal applications should have little
    /// reason to call this method. Yet it may be useful for writing bindings which copy directly
    /// from the ODBC in memory representation into other kinds of buffers.
    ///
    /// The buffer contains the bytes for every non null valid element, padded to the maximum string
    /// length. The content of the padding bytes is undefined. Usually ODBC drivers write a
    /// terminating zero at the end of each string. For the actual value length call
    /// [`Self::content_length_at`]. Any element starts at index * ([`Self::max_len`] + 1).
    pub fn raw_value_buffer(&self) -> &'c [C] {
        self.col.raw_value_buffer(self.num_rows)
    }

    pub fn max_len(&self) -> usize {
        self.col.max_len()
    }

    /// `Some` if any value is truncated.
    ///
    /// After fetching data we may want to know if any value has been truncated due to the buffer
    /// not being able to hold elements of that size. This method checks the indicator buffer
    /// element wise.
    pub fn has_truncated_values(&self) -> Option<Indicator> {
        self.col.has_truncated_values(self.num_rows)
    }
}

unsafe impl<'a, C: 'static> BoundInputSlice<'a> for TextColumn<C> {
    type SliceMut = TextColumnSliceMut<'a, C>;

    unsafe fn as_view_mut(
        &'a mut self,
        parameter_index: u16,
        stmt: StatementRef<'a>,
    ) -> Self::SliceMut {
        TextColumnSliceMut {
            column: self,
            stmt,
            parameter_index,
        }
    }
}

/// A view to a mutable array parameter text buffer, which allows for filling the buffer with
/// values.
pub struct TextColumnSliceMut<'a, C> {
    column: &'a mut TextColumn<C>,
    // Needed to rebind the column in case of resize
    stmt: StatementRef<'a>,
    // Also needed to rebind the column in case of resize
    parameter_index: u16,
}

impl<C> TextColumnSliceMut<'_, C>
where
    C: Default + Copy,
{
    /// Sets the value of the buffer at index at Null or the specified binary Text. This method will
    /// panic on out of bounds index, or if input holds a text which is larger than the maximum
    /// allowed element length. `element` must be specified without the terminating zero.
    pub fn set_cell(&mut self, row_index: usize, element: Option<&[C]>) {
        self.column.set_value(row_index, element)
    }

    /// Ensures that the buffer is large enough to hold elements of `element_length`. Does nothing
    /// if the buffer is already large enough. Otherwise it will reallocate and rebind the buffer.
    /// The first `num_rows_to_copy` will be copied from the old value buffer to the new
    /// one. This makes this an extremely expensive operation.
    pub fn ensure_max_element_length(
        &mut self,
        element_length: usize,
        num_rows_to_copy: usize,
    ) -> Result<(), Error>
    where
        TextColumn<C>: HasDataType + CData,
    {
        // Column buffer is not large enough to hold the element. We must allocate a larger buffer
        // in order to hold it. This invalidates the pointers previously bound to the statement. So
        // we rebind them.
        if element_length > self.column.max_len() {
            let new_max_str_len = element_length;
            self.column
                .resize_max_str(new_max_str_len, num_rows_to_copy);
            unsafe {
                self.stmt
                    .bind_input_parameter(self.parameter_index, self.column)
                    .into_result(&self.stmt)?
            }
        }
        Ok(())
    }

    /// Can be used to set a value at a specific row index without performing a memcopy on an input
    /// slice and instead provides direct access to the underlying buffer.
    ///
    /// In situations there the memcopy can not be avoided anyway [`Self::set_cell`] is likely to
    /// be more convenient. This method is very useful if you want to `write!` a string value to the
    /// buffer and the binary (**!**) length of the formatted string is known upfront.
    ///
    /// # Example: Write timestamp to text column.
    ///
    /// ```
    /// use odbc_api::buffers::TextColumnSliceMut;
    /// use std::io::Write;
    ///
    /// /// Writes times formatted as hh::mm::ss.fff
    /// fn write_time(
    ///     col: &mut TextColumnSliceMut<u8>,
    ///     index: usize,
    ///     hours: u8,
    ///     minutes: u8,
    ///     seconds: u8,
    ///     milliseconds: u16)
    /// {
    ///     write!(
    ///         col.set_mut(index, 12),
    ///         "{:02}:{:02}:{:02}.{:03}",
    ///         hours, minutes, seconds, milliseconds
    ///     ).unwrap();
    /// }
    /// ```
    pub fn set_mut(&mut self, index: usize, length: usize) -> &mut [C] {
        self.column.set_mut(index, length)
    }
}

/// Iterator over a text column. See [`TextColumnView::iter`]
#[derive(Debug)]
pub struct TextColumnIt<'c, C> {
    pos: usize,
    num_rows: usize,
    col: &'c TextColumn<C>,
}

impl<'c, C> TextColumnIt<'c, C> {
    fn next_impl(&mut self) -> Option<Option<&'c [C]>> {
        if self.pos == self.num_rows {
            None
        } else {
            let ret = Some(self.col.value_at(self.pos));
            self.pos += 1;
            ret
        }
    }
}

impl<'c> Iterator for TextColumnIt<'c, u8> {
    type Item = Option<&'c [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.num_rows - self.pos;
        (len, Some(len))
    }
}

impl ExactSizeIterator for TextColumnIt<'_, u8> {}

impl<'c> Iterator for TextColumnIt<'c, u16> {
    type Item = Option<&'c U16Str>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl().map(|opt| opt.map(U16Str::from_slice))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.num_rows - self.pos;
        (len, Some(len))
    }
}

impl ExactSizeIterator for TextColumnIt<'_, u16> {}

unsafe impl CData for CharColumn {
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

unsafe impl CDataMut for CharColumn {
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        self.indicators.as_mut_ptr()
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.values.as_mut_ptr() as *mut c_void
    }
}

impl HasDataType for CharColumn {
    fn data_type(&self) -> DataType {
        DataType::Varchar {
            length: NonZeroUsize::new(self.max_str_len),
        }
    }
}

unsafe impl CData for WCharColumn {
    fn cdata_type(&self) -> CDataType {
        CDataType::WChar
    }

    fn indicator_ptr(&self) -> *const isize {
        self.indicators.as_ptr()
    }

    fn value_ptr(&self) -> *const c_void {
        self.values.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> isize {
        ((self.max_str_len + 1) * 2).try_into().unwrap()
    }
}

unsafe impl CDataMut for WCharColumn {
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        self.indicators.as_mut_ptr()
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.values.as_mut_ptr() as *mut c_void
    }
}

impl HasDataType for WCharColumn {
    fn data_type(&self) -> DataType {
        DataType::WVarchar {
            length: NonZeroUsize::new(self.max_str_len),
        }
    }
}

use std::{
    borrow::{Borrow, BorrowMut},
    ffi::c_void,
};

use odbc_sys::{CDataType, NULL_DATA};

use crate::{
    buffers::Indicator,
    handles::{CData, CDataMut, HasDataType},
    parameter::InputParameter,
    DataType, OutputParameter,
};

use super::CElement;

/// Binds a byte array as Variadic sized binary data. It can not be used for columnar bulk fetches,
/// but if the buffer type is stack allocated it can be utilized in row wise bulk fetches.
///
/// Meaningful instantiations of this type are:
///
/// * [`self::VarBinarySlice`] - immutable borrowed parameter.
/// * [`self::VarBinarySliceMut`] - mutable borrowed input / output parameter
/// * [`self::VarBinaryArray`] - stack allocated owned input / output parameter
/// * [`self::VarBinaryBox`] - heap allocated owned input /output parameter
#[derive(Debug, Clone, Copy)]
pub struct VarBinary<B> {
    /// Contains the value. Bytes must be valid up to the index indicated by `indicator`. If
    /// `indicator` is longer than buffer the value has been truncated and all bytes are considered
    /// part of the payload.
    buffer: B,
    /// Indicates the length of the value stored in `buffer`. Should indicator exceed the buffer
    /// length the value stored in buffer is truncated, and holds actually `buffer.len()` valid
    /// bytes.
    indicator: isize,
}

/// Parameter type for owned, variable sized binary data.
///
/// We use `Box<[u8]>` rather than `Vec<u8>` as a buffer type since the indicator pointer already
/// has the role of telling us how many bytes in the buffer are part of the payload.
pub type VarBinaryBox = VarBinary<Box<[u8]>>;

impl VarBinaryBox {
    /// Constructs a 'missing' value.
    pub fn null() -> Self {
        // Insert 0 in buffer to avoid binding as VARBINARY(0)
        Self::from_buffer(Box::new([0]), Indicator::Null)
    }

    /// Create an instance from a `Vec`.
    pub fn from_vec(val: Vec<u8>) -> Self {
        let indicator = Indicator::Length(val.len());
        let buffer = val.into_boxed_slice();
        Self::from_buffer(buffer, indicator)
    }
}

impl<B> VarBinary<B>
where
    B: Borrow<[u8]>,
{
    /// Creates a new instance from an existing buffer.
    pub fn from_buffer(buffer: B, indicator: Indicator) -> Self {
        Self {
            buffer,
            indicator: indicator.to_isize(),
        }
    }

    /// Valid payload of the buffer returned as slice or `None` in case the indicator is
    /// `NULL_DATA`.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        let slice = self.buffer.borrow();
        match self.indicator() {
            Indicator::Null => None,
            Indicator::NoTotal => Some(slice),
            Indicator::Length(len) => {
                if self.is_complete() {
                    Some(&slice[..len])
                } else {
                    Some(slice)
                }
            }
        }
    }

    /// Call this method to ensure that the entire field content did fit into the buffer. If you
    /// retrieve a field using [`crate::CursorRow::get_data`], you can repeat the call until this
    /// method is false to read all the data.
    ///
    /// ```
    /// use odbc_api::{CursorRow, parameter::VarBinaryArray, Error, handles::Statement};
    ///
    /// fn process_large_binary(
    ///     col_index: u16,
    ///     row: &mut CursorRow<'_>
    /// ) -> Result<(), Error>{
    ///     let mut buf = VarBinaryArray::<512>::NULL;
    ///     row.get_data(col_index, &mut buf)?;
    ///     while !buf.is_complete() {
    ///         // Process bytes in stream without allocation. We can assume repeated calls to
    ///         // get_data do not return `None` since it would have done so on the first call.
    ///         process_slice(buf.as_bytes().unwrap());
    ///     }
    ///     Ok(())
    /// }
    ///
    /// fn process_slice(text: &[u8]) { /*...*/}
    ///
    /// ```
    pub fn is_complete(&self) -> bool {
        let slice = self.buffer.borrow();
        match self.indicator() {
            Indicator::Null => true,
            Indicator::NoTotal => false,
            Indicator::Length(len) => len <= slice.len(),
        }
    }

    /// Read access to the underlying ODBC indicator. After data has been fetched the indicator
    /// value is set to the length the buffer should have had to hold the entire value. It may also
    /// be [`Indicator::Null`] to indicate `NULL` or [`Indicator::NoTotal`] which tells us the data
    /// source does not know how big the buffer must be to hold the complete value.
    /// [`Indicator::NoTotal`] implies that the content of the current buffer is valid up to its
    /// maximum capacity.
    pub fn indicator(&self) -> Indicator {
        Indicator::from_isize(self.indicator)
    }
}

impl<B> VarBinary<B>
where
    B: Borrow<[u8]>,
{
    /// Call this method to reset the indicator to a value which matches the length returned by the
    /// [`Self::as_bytes`] method. This is useful if you want to insert values into the database
    /// despite the fact, that they might have been truncated.
    pub fn hide_truncation(&mut self) {
        if !self.is_complete() {
            self.indicator = self.buffer.borrow().len().try_into().unwrap();
        }
    }
}

unsafe impl<B> CData for VarBinary<B>
where
    B: Borrow<[u8]>,
{
    fn cdata_type(&self) -> CDataType {
        CDataType::Binary
    }

    fn indicator_ptr(&self) -> *const isize {
        &self.indicator as *const isize
    }

    fn value_ptr(&self) -> *const c_void {
        self.buffer.borrow().as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> isize {
        // This is the maximum buffer length, but it is NOT the length of an instance of Self due to
        // the missing size of the indicator value. As such the buffer length can not be used to
        // correctly index a columnar buffer of Self.
        self.buffer.borrow().len().try_into().unwrap()
    }
}

impl<B> HasDataType for VarBinary<B>
where
    B: Borrow<[u8]>,
{
    fn data_type(&self) -> DataType {
        DataType::Varbinary {
            length: self.buffer.borrow().len(),
        }
    }
}

unsafe impl<B> CDataMut for VarBinary<B>
where
    B: BorrowMut<[u8]>,
{
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        &mut self.indicator as *mut isize
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.buffer.borrow_mut().as_mut_ptr() as *mut c_void
    }
}

/// Binds a byte array as a variadic binary input parameter.
///
/// While a byte array can provide us with a pointer to the start of the array and the length of the
/// array itself, it can not provide us with a pointer to the length of the buffer. So to bind
/// byte slices (`&[u8]`) we need to store the length in a separate value.
///
/// This type is created if `into_parameter` of the `IntoParameter` trait is called on a `&[u8]`.
pub type VarBinarySlice<'a> = VarBinary<&'a [u8]>;

impl<'a> VarBinarySlice<'a> {
    /// Indicates missing data
    pub const NULL: Self = Self {
        // Insert 0 in buffer to avoid binding as VARBINARY(0)
        buffer: &[0],
        indicator: NULL_DATA,
    };

    /// Constructs a new instance containing the bytes in the specified buffer.
    pub fn new(value: &'a [u8]) -> Self {
        Self::from_buffer(value, Indicator::Length(value.len()))
    }
}

/// Wraps a slice so it can be used as an output parameter for binary data.
pub type VarBinarySliceMut<'a> = VarBinary<&'a mut [u8]>;

/// A stack allocated VARBINARY type.
///
/// Due to its memory layout this type can be bound either as a single parameter, or as a column of
/// a row-by-row output, but not be used in columnar parameter arrays or output buffers.
pub type VarBinaryArray<const LENGTH: usize> = VarBinary<[u8; LENGTH]>;

impl<const LENGTH: usize> VarBinaryArray<LENGTH> {
    /// Indicates a missing value.
    pub const NULL: Self = VarBinaryArray {
        buffer: [0; LENGTH],
        indicator: NULL_DATA,
    };

    /// Construct from a slice. If value is longer than `LENGTH` it will be truncated.
    pub fn new(bytes: &[u8]) -> Self {
        let indicator = bytes.len().try_into().unwrap();
        let mut buffer = [0u8; LENGTH];
        if bytes.len() > LENGTH {
            buffer.copy_from_slice(&bytes[..LENGTH]);
        } else {
            buffer[..bytes.len()].copy_from_slice(bytes);
        };
        Self { buffer, indicator }
    }
}

// We can't go all out and implement these traits for anything implementing Borrow and BorrowMut,
// because erroneous but still safe implementation of these traits could cause invalid memory access
// down the road. E.g. think about returning a different slice with a different length for borrow
// and borrow_mut.
unsafe impl CElement for VarBinarySlice<'_> {}
unsafe impl InputParameter for VarBinarySlice<'_> {}

unsafe impl<const LENGTH: usize> CElement for VarBinaryArray<LENGTH> {}
unsafe impl<const LENGTH: usize> OutputParameter for VarBinaryArray<LENGTH> {}
unsafe impl<const LENGTH: usize> InputParameter for VarBinaryArray<LENGTH> {}

unsafe impl CElement for VarBinarySliceMut<'_> {}
unsafe impl OutputParameter for VarBinarySliceMut<'_> {}
unsafe impl InputParameter for VarBinarySliceMut<'_> {}

unsafe impl CElement for VarBinaryBox {}
unsafe impl OutputParameter for VarBinaryBox {}
unsafe impl InputParameter for VarBinaryBox {}

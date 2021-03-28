use std::{
    borrow::{Borrow, BorrowMut},
    convert::TryInto,
    ffi::c_void,
};

use odbc_sys::{CDataType, NULL_DATA};

use crate::{
    buffers::Indicator,
    handles::{CData, CDataMut, HasDataType},
    DataType, InputParameter, Output,
};

/// Binds a byte arrary as Variadic sized characater data. It can not be used for columnar bulk
/// fetches, but if the buffer type is stack allocated it can be utilized in row wise bulk fetches.
///
/// Meaningful instantiations of this type are:
///
/// * [`self::VarCharSlice`] - immutable borrowed parameter.
/// * [`self::VarCharSliceMut`] - mutable borrowed input / output parameter
/// * [`self::VarCharArray`] - stack allocated owned input / output parameter
/// * [`self::VarCharBox`] - heap allocated owned input /output parameter
#[derive(Debug, Clone, Copy)]
pub struct VarChar<B> {
    /// Contains the value. Characters must be valid up to the index indicated by `indicator`. If
    /// `indicator` is longer than buffer, the last element in buffer must be a terminating zero,
    /// which is not regarded as being part of the payload itself.
    buffer: B,
    /// Indicates the length of the value stored in `buffer`. Should indicator exceed the buffer
    /// length the value stored in buffer is truncated, and holds actually `buffer.len() - 1` valid
    /// characters. The last element of the buffer being the terminating zero. If indicator is
    /// exactly the buffer length, the value should be considered valid up to the last element,
    /// unless the value is `\0`. In that case we assume `\0` to be a terminating zero left over
    /// from truncation, rather than the last character of the string.
    indicator: isize,
}

/// Binds a byte array as a VarChar input parameter.
///
/// While a byte array can provide us with a pointer to the start of the array and the length of the
/// array itself, it can not provide us with a pointer to the length of the buffer. So to bind
/// strings which are not zero terminated we need to store the length in a separate value.
///
/// This type is created if `into_parameter` of the `IntoParameter` trait is called on a `&str`.
///
/// # Example
///
/// ```no_run
/// use odbc_api::{Environment, IntoParameter};
///
/// let env = unsafe {
///     Environment::new()?
/// };
///
/// let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
/// if let Some(cursor) = conn.execute(
///     "SELECT year FROM Birthdays WHERE name=?;",
///     &"Bernd".into_parameter())?
/// {
///     // Use cursor to process query results.
/// };
/// # Ok::<(), odbc_api::Error>(())
/// ```
pub type VarCharSlice<'a> = VarChar<&'a [u8]>;

impl<'a> VarCharSlice<'a> {
    /// Indicates missing data
    pub const NULL: Self = Self {
        buffer: &[],
        indicator: NULL_DATA,
    };

    /// Constructs a new VarChar containing the text in the specified buffer.
    ///
    /// Caveat: This constructor is going to create a truncated value in case the input slice ends
    /// with `nul`. Should you want to insert an actual string those payload ends with `nul` into
    /// the database you need a buffer one byte longer than the string. You can instantiate such a
    /// value using [`Self::from_buffer`].
    pub fn new(value: &'a [u8]) -> Self {
        Self::from_buffer(value, Indicator::Length(value.len()))
    }
}

/// Wraps a slice so it can be used as an output parameter for character data.
pub type VarCharSliceMut<'a> = VarChar<&'a mut [u8]>;

/// A stack allocated VARCHAR type able to hold strings up to a length of 32 bytes (including the
/// terminating zero for output strings).
///
/// Due to its memory layout this type can be bound either as a single parameter, or as a column of
/// a row-by-row output, but not be used in columnar parameter arrays or output buffers.
pub type VarCharArray<const LENGTH: usize> = VarChar<[u8; LENGTH]>;

impl<const LENGTH: usize> VarCharArray<LENGTH> {
    /// Indicates a missing value.
    pub const NULL: Self = VarCharArray {
        buffer: [0; LENGTH],
        indicator: NULL_DATA,
    };

    /// Construct from a slice. If value is longer than `LENGTH` it will be truncated and a
    /// a terminating zero is placed at the end.
    pub fn new(text: &[u8]) -> Self {
        let indicator = text.len().try_into().unwrap();
        let mut buffer = [0u8; LENGTH];
        if text.len() > LENGTH {
            buffer.copy_from_slice(&text[..LENGTH]);
        } else {
            buffer[..text.len()].copy_from_slice(text);
        };
        Self { indicator, buffer }
    }
}

impl<B> VarChar<B>
where
    B: Borrow<[u8]>,
{
    /// Creates a new instance from an existing buffer. Shoud the indicator be `NoTotal` or indicate
    /// a length longer than buffer, the last element in the buffer must be nul (`\0`).
    pub fn from_buffer(buffer: B, indicator: Indicator) -> Self {
        let buf = buffer.borrow();
        match indicator {
            Indicator::Null => (),
            Indicator::NoTotal => {
                if buf.is_empty() || *buf.last().unwrap() != 0 {
                    panic!("Truncated value must be terminated with zero.")
                }
            }
            Indicator::Length(len) => {
                if len > buf.len() && (buf.is_empty() || *buf.last().unwrap() != 0) {
                    panic!("Truncated value must be terminated with zero.")
                }
            }
        };
        Self {
            buffer,
            indicator: indicator.to_isize(),
        }
    }

    /// Returns the binary representation of the string, excluding the terminating zero or `None` in
    /// case the indicator is `NULL_DATA`.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        let slice = self.buffer.borrow();
        match self.indicator() {
            Indicator::Null => None,
            Indicator::NoTotal => Some(&slice[..(slice.len() - 1)]),
            Indicator::Length(len) => {
                if self.is_complete() {
                    Some(&slice[..len])
                } else {
                    Some(&slice[..(slice.len() - 1)])
                }
            }
        }
    }

    /// Call this method to ensure that the entire field content did fit into the buffer. If you
    /// retrieve a field using [`crate::CursorRow::get_data`], you can repeat the call until this
    /// method is false to read all the data.
    ///
    /// ```
    /// use odbc_api::{CursorRow, parameter::VarCharArray, Error, handles::Statement};
    ///
    /// fn process_large_text<S: Statement>(
    ///     col_index: u16,
    ///     row: &mut CursorRow<S>
    /// ) -> Result<(), Error>{
    ///     let mut buf = VarCharArray::<512>::NULL;
    ///     row.get_data(col_index, &mut buf)?;
    ///     while !buf.is_complete() {
    ///         // Process bytes in stream without allocation. We can assume repeated calls to
    ///         // get_data do not return `None` since it would have done so on the first call.
    ///         process_text_slice(buf.as_bytes().unwrap());
    ///     }
    ///     Ok(())
    /// }
    ///
    /// fn process_text_slice(text: &[u8]) { /*...*/}
    ///
    /// ```
    pub fn is_complete(&self) -> bool {
        let slice = self.buffer.borrow();
        match self.indicator() {
            Indicator::Null => true,
            Indicator::NoTotal => false,
            Indicator::Length(len) => {
                len < slice.len() || slice.is_empty() || *slice.last().unwrap() != 0
            }
        }
    }

    /// Read access to the underlying ODBC indicator. After data has been fetched the indicator
    /// value is set to the length the buffer should have had, excluding the terminating zero. It
    /// may also be `NULL_DATA` to indicate `NULL` or `NO_TOTAL` which tells us the data source
    /// does not know how big the buffer must be to hold the complete value. `NO_TOTAL` implies that
    /// the content of the current buffer is valid up to its maximum capacity.
    pub fn indicator(&self) -> Indicator {
        Indicator::from_isize(self.indicator)
    }
}

impl<B> VarChar<B>
where
    B: Borrow<[u8]>,
{
    /// Call this method to reset the indicator to a value which matches the length returned by the
    /// [`Self::bytes`] method. This is useful if you want to insert values into the database
    /// despite the fact, that they might have been truncated. Otherwise the behaviour of databases
    /// in this situation is driver specific. Some drivers insert up to the terminating zero, others
    /// detect the truncation and throw an error.
    pub fn hide_truncation(&mut self) {
        if !self.is_complete() {
            self.indicator = (self.buffer.borrow().len() - 1).try_into().unwrap();
        }
    }
}

unsafe impl<B> CData for VarChar<B>
where
    B: Borrow<[u8]>,
{
    fn cdata_type(&self) -> CDataType {
        CDataType::Char
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

unsafe impl<B> HasDataType for VarChar<B>
where
    B: Borrow<[u8]>,
{
    fn data_type(&self) -> DataType {
        // Since we might use as an input buffer, we report the full buffer length in the type and
        // do not deduct 1 for the terminating zero.
        DataType::Varchar {
            length: self.buffer.borrow().len(),
        }
    }
}

unsafe impl<B> CDataMut for VarChar<B>
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

// We can't go all out and implement these traits for anything implementing Borrow and BorrowMut,
// because erroneous but still safe implementation of these traits could cause invalid memory access
// down the road. E.g. think about returning a different slice with a different length for borrow
// and borrow_mut.

unsafe impl InputParameter for VarCharSlice<'_> {}

unsafe impl<const LENGTH: usize> Output for VarCharArray<LENGTH> {}
unsafe impl<const LENGTH: usize> InputParameter for VarCharArray<LENGTH> {}

unsafe impl<'a> Output for VarCharSliceMut<'a> {}
unsafe impl<'a> InputParameter for VarCharSliceMut<'a> {}

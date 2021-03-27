use std::{borrow::{Borrow, BorrowMut}, convert::TryInto, ffi::c_void};

use odbc_sys::{CDataType, NULL_DATA, NO_TOTAL};

use crate::{DataType, InputParameter, Output, handles::{CData, CDataMut, HasDataType}};

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
pub struct VarCharRef<'a> {
    bytes: &'a [u8],
    /// Will be set to value.len() by constructor.
    length: isize,
}

impl<'a> VarCharRef<'a> {
    /// Constructs a new VarChar containing the text in the specified buffer.
    pub fn new(value: &'a [u8]) -> Self {
        VarCharRef {
            bytes: value,
            length: value.len().try_into().unwrap(),
        }
    }

    /// Constructs a new VarChar representing the NULL value.
    pub fn null() -> Self {
        VarCharRef {
            bytes: &[],
            length: NULL_DATA,
        }
    }
}

unsafe impl CData for VarCharRef<'_> {
    fn cdata_type(&self) -> CDataType {
        CDataType::Char
    }

    fn indicator_ptr(&self) -> *const isize {
        &self.length
    }

    fn value_ptr(&self) -> *const c_void {
        self.bytes.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> isize {
        0
    }
}

unsafe impl HasDataType for VarCharRef<'_> {
    fn data_type(&self) -> DataType {
        DataType::Varchar {
            length: self.bytes.len(),
        }
    }
}

unsafe impl InputParameter for VarCharRef<'_> {}

/// A stack allocated VARCHAR type able to hold strings up to a length of 32 bytes (including the
/// terminating zero).
///
/// Due to its memory layout this type can be bound either as a single parameter, or as a column of
/// a row-by-row output, but not be used in columnar parameter arrays or output buffers.
pub type VarChar32 = VarChar<[u8; 32]>;

/// A stack allocated VARCHAR type able to hold strings up to a length of 512 bytes (including the
/// terminating zero).
///
/// Due to its memory layout this type can be bound either as a single parameter, or as a column of
/// of a row-by-row output, but not be used in columnar parameter arrays or output buffers.
pub type VarChar512 = VarChar<[u8; 512]>;

/// Wraps a slice so it can be used as an output parameter for character data.
pub type VarCharMut<'a> = VarChar<&'a mut [u8]>;

/// A mutable buffer for character data which can be used as either input parameter or output
/// buffer. It can not be used for columnar bulk fetches, but if the buffer type is stack allocated
/// in can be utilized in row wise bulk fetches.
///
/// This type is very similar to [`self::VarCharRef`] and indeed it can perform many of the same
/// tasks, since [`self::VarCharRef`] is exclusive used as an input parameter though it must not
/// account for a terminating zero at the end of the buffer.
#[derive(Debug, Clone, Copy)]
pub struct VarChar<B> {
    buffer: B,
    indicator: isize,
}

impl<B> VarChar<B>
where
    B: BorrowMut<[u8]>,
{
    /// Creates a new instance. It takes ownership of the buffer. The indicator tells us up to which
    /// position the buffer is filled. Pass `None` for the indicator to create a value representing
    /// `NULL`. The constructor will write a terminating zero after the end of the valid sequence in
    /// the buffer.
    pub fn from_buffer(mut buffer: B, indicator: Option<usize>) -> Self {
        if let Some(indicator) = indicator {
            // Insert terminating zero
            buffer.borrow_mut()[indicator + 1] = 0;
            let indicator: isize = indicator.try_into().unwrap();
            VarChar { buffer, indicator }
        } else {
            VarChar {
                buffer,
                indicator: NULL_DATA,
            }
        }
    }

    /// Construct a new VarChar and copy the value of the slice into the internal buffer. `None`
    /// indicates `NULL`.
    pub fn copy_from_bytes(bytes: Option<&[u8]>) -> Self
    where
        B: Default,
    {
        let mut buffer = B::default();
        if let Some(bytes) = bytes {
            let slice = buffer.borrow_mut();
            if bytes.len() > slice.len() - 1 {
                panic!("Value is to large to be stored in a VarChar512");
            }
            slice[..bytes.len()].copy_from_slice(bytes);
            Self::from_buffer(buffer, Some(bytes.len()))
        } else {
            Self::from_buffer(buffer, None)
        }
    }

    /// Returns the binary representation of the string, excluding the terminating zero.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        let slice = self.buffer.borrow();
        let max: isize = slice.len().try_into().unwrap();
        match self.indicator {
            NULL_DATA => None,
            complete if complete < max => Some(&slice[..(self.indicator as usize)]),
            // This case includes both: indicators larger than max and `NO_TOTAL`
            _ => Some(&slice[..(slice.len() - 1)]),
        }
    }

    /// Call this method to ensure that the entire field content did fit into the buffer. If you
    /// retrieve a field using [`crate::CursorRow::get_data`], you can repeat the call until this
    /// method is false to read all the data.
    ///
    /// ```
    /// use odbc_api::{CursorRow, parameter::VarChar512, Error, handles::Statement};
    ///
    /// fn process_large_text<S: Statement>(
    ///     col_index: u16,
    ///     row: &mut CursorRow<S>
    /// ) -> Result<(), Error>{
    ///     let mut buf = VarChar512::from_buffer([0;512], None);
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
        match self.indicator {
            NULL_DATA => true,
            NO_TOTAL => false,
            other => {
                let other: usize = other.try_into().unwrap();
                other < self.buffer.borrow().len()
            }
        }
    }

    /// Read access to the underlying ODBC indicator. After data has been fetched the indicator
    /// value is set to the length the buffer should have had, excluding the terminating zero. It
    /// may also be `NULL_DATA` to indicate `NULL` or `NO_TOTAL` which tells us the data source
    /// does not know how big the buffer must be to hold the complete value. `NO_TOTAL` implies that
    /// the content of the current buffer is valid up to its maximum capacity.
    pub fn indicator(&self) -> isize {
        self.indicator
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
        // Buffer length minus 1 for terminating zero
        DataType::Varchar {
            length: self.buffer.borrow().len() - 1,
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

unsafe impl Output for VarChar512 {}
unsafe impl InputParameter for VarChar512 {}

unsafe impl Output for VarChar32 {}
unsafe impl InputParameter for VarChar32 {}

unsafe impl<'a> Output for VarCharMut<'a> {}
unsafe impl<'a> InputParameter for VarCharMut<'a> {}

// For completeness sake. VarCharRef will do the same job slightly better though.
unsafe impl<'a> InputParameter for VarChar<&'a [u8]> {}

#[cfg(test)]
mod tests {
    use super::VarChar;

    #[test]
    #[should_panic]
    fn construct_to_large_varchar_512() {
        VarChar::<[u8; 32]>::copy_from_bytes(Some(&vec![b'a'; 32]));
    }
}
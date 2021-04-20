use std::{cmp::min, convert::TryInto, ptr::{null, null_mut}};

use widestring::U16CStr;

/// Clamps a usize between `0` and `SmallInt::MAX`.
pub fn clamp_small_int(n: usize) -> i16 {
    min(n, i16::MAX as usize) as i16
}

/// Returns a pointer suitable to be passed as an output buffer to ODBC functions. Most notably it
/// will return NULL for empty buffers.
pub fn mut_buf_ptr<T>(buffer: &mut [T]) -> *mut T {
    if buffer.is_empty() {
        null_mut()
    } else {
        buffer.as_mut_ptr()
    }
}

/// Returns a pointer suitable to be passed as an output buffer to ODBC functions. Most notably it
/// will return NULL for empty buffers.
pub fn buf_ptr<T>(buffer: &[T]) -> *const T {
    if buffer.is_empty() {
        null()
    } else {
        buffer.as_ptr()
    }
}

/// We use this as an output buffer for strings. Allows for detecting truncation.
pub struct OutputStringBuffer {
    /// Buffer holding the string. This crate uses wide methods (i.e. UTF-16) so we allocate a
    /// `Vec<u16>` instead of a `Vec<u8>`. Must also contain space for a terminating zero.
    buffer: Vec<u16>,
    /// After the buffer has been filled, this should contain the actual length of the string. Can
    /// be used to detect truncation.
    actual_length: i16,
}

impl OutputStringBuffer {

    /// Creates a new instance of an output string buffer which can hold strings up to a size of
    /// `max_str_len` characters.
    pub fn with_buffer_size(max_str_len: usize) -> Self {
        Self {
            buffer: vec![0; max_str_len + 1],
            actual_length: 0,
        }
    }

    /// Ptr to the internal buffer. Used by ODBC API calls to fill the buffer.
    pub fn mut_buf_ptr(&mut self) -> *mut u16{
        mut_buf_ptr(&mut self.buffer)
    }

    /// Length of the internal buffer in characters excluding the terminating zero.
    pub fn buf_len(&self) -> i16 {
        if self.buffer.is_empty() {
            0
        } else {
            (self.buffer.len() - 1).try_into().unwrap()
        }
    }

    /// Mutable pointer to actual output string length. Used by ODBC API calls to report truncation.
    pub fn mut_actual_len_ptr(&mut self) -> * mut i16 {
        &mut self.actual_length as * mut i16
    }

    /// Interpret buffer as UTF16 string. Panics if no terminating zero present.
    pub fn as_ucstr(&self) -> &U16CStr {
        U16CStr::from_slice_with_nul(&self.buffer).unwrap()
    }

    /// Call this method to extract string from buffer after ODBC has filled it.
    pub fn to_string(&self) -> String {
        self.as_ucstr().to_string().unwrap()
    }

    /// True if the buffer had not been large enough to hold the string.
    pub fn is_truncated(&self) -> bool {
        let len: usize = self.actual_length.try_into().unwrap();
        // One character is needed for the terminating zero, but string size is reported in
        // characters without terminating zero.
        len >= self.buffer.len()
    }
}
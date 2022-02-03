//! The idea is to handle most of the conditional compilation around different SQL character types
//! in this module, so the rest of the crate doesn't have to.

use super::buffer::{buf_ptr, mut_buf_ptr};
use std::mem::size_of;

#[cfg(feature = "narrow")]
use std::{ffi::CStr, string::FromUtf8Error};

#[cfg(not(feature = "narrow"))]
use std::{
    char::{decode_utf16, DecodeUtf16Error},
    marker::PhantomData,
};

#[cfg(not(feature = "narrow"))]
use widestring::{U16CStr, U16String};

#[cfg(feature = "narrow")]
pub type SqlChar = u8;
#[cfg(not(feature = "narrow"))]
pub type SqlChar = u16;

#[cfg(feature = "narrow")]
pub type DecodingError = FromUtf8Error;
#[cfg(not(feature = "narrow"))]
pub type DecodingError = DecodeUtf16Error;

#[cfg(feature = "narrow")]
pub fn slice_to_utf8(text: &[u8]) -> Result<String, FromUtf8Error> {
    String::from_utf8(text.to_owned())
}
#[cfg(not(feature = "narrow"))]
pub fn slice_to_utf8(text: &[u16]) -> Result<String, DecodeUtf16Error> {
    decode_utf16(text.iter().copied()).collect()
}

/// Buffer length in bytes, not characters
pub fn binary_length(buffer: &[SqlChar]) -> usize {
    buffer.len() * size_of::<SqlChar>()
}

/// `true` if the buffer has not been large enough to hold the entire string.
///
/// # Parameters
///
/// - `actuel_length_bin`: Actual length in bytes, but excluding the terminating zero.
pub fn is_truncated_bin(buffer: &[SqlChar], actual_length_bin: usize) -> bool {
    buffer.len() * size_of::<SqlChar>() <= actual_length_bin
}

/// Resizes the underlying buffer to fit the size required to hold the entire string including
/// terminating zero. Required length is provided in bytes (not characters), excluding the
/// terminating zero.
pub fn resize_to_fit_with_tz(buffer: &mut Vec<SqlChar>, required_binary_length: usize) {
    buffer.resize((required_binary_length / size_of::<SqlChar>()) + 1, 0);
}

/// Resizes the underlying buffer to fit the size required to hold the entire string excluding
/// terminating zero. Required length is provided in bytes (not characters), excluding the
/// terminating zero.
pub fn resize_to_fit_without_tz(buffer: &mut Vec<SqlChar>, required_binary_length: usize) {
    buffer.resize(required_binary_length / size_of::<SqlChar>(), 0);
}

/// Handles conversion from UTF-8 string slices to ODBC SQL char encoding. Depending on the
/// conditional compiliation due to feature flags, the UTF-8 strings are either passed without
/// conversion to narrow method calls, or they are converted to UTF-16, before passed to the wide
/// methods.
pub struct SqlText<'a> {
    /// In case we use wide methods we need to convert to UTF-16. We'll take ownership of the buffer
    /// here.
    #[cfg(not(feature = "narrow"))]
    text: U16String,
    /// We include the lifetime in the declaration of the type still, so the borrow checker
    /// complains, if we would mess up the compilation for narrow methods.
    #[cfg(not(feature = "narrow"))]
    _ref: PhantomData<&'a str>,
    /// In the case of narrow compiliation we just forward the string silce unchanged
    #[cfg(feature = "narrow")]
    text: &'a str,
}

impl<'a> SqlText<'a> {
    #[cfg(not(feature = "narrow"))]
    /// Create an SqlText buffer from an UTF-8 string slice
    pub fn new(text: &'a str) -> Self {
        Self {
            text: U16String::from_str(text),
            _ref: PhantomData,
        }
    }
    #[cfg(feature = "narrow")]
    /// Create an SqlText buffer from an UTF-8 string slice
    pub fn new(text: &'a str) -> Self {
        Self { text }
    }

    #[cfg(not(feature = "narrow"))]
    pub fn ptr(&self) -> *const u16 {
        buf_ptr(self.text.as_slice())
    }
    #[cfg(feature = "narrow")]
    pub fn ptr(&self) -> *const u8 {
        buf_ptr(self.text.as_bytes())
    }

    /// Length in characters
    pub fn len_char(&self) -> i16 {
        self.text.len().try_into().unwrap()
    }
}

/// Use this buffer type to fetch zero terminated strings from the ODBC API. Either allocates a
/// buffer for wide or narrow strings dependend on the features set.
pub struct SzBuffer {
    buffer: Vec<SqlChar>,
}

impl SzBuffer {
    /// Creates a buffer which can hold at least `capacity` characters, excluding the terminating
    /// zero. Or phrased differently. It will allocate one additional character to hold the
    /// terminating zero, so the caller should not factor it into the size of capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            // Allocate +1 character extra for terminating zero
            buffer: vec![0; capacity + 1],
        }
    }

    pub fn mut_buf(&mut self) -> &mut [SqlChar] {
        // Use full capacity
        self.buffer.resize(self.buffer.capacity(), 0);
        &mut self.buffer
    }

    #[cfg(not(feature = "narrow"))]
    pub fn to_utf8(&self) -> String {
        let c_str = U16CStr::from_slice_truncate(&self.buffer).unwrap();
        c_str.to_string_lossy()
    }
    #[cfg(feature = "narrow")]
    pub fn to_utf8(&self) -> String {
        // Truncate slice at first zero.
        let end = self
            .buffer
            .iter()
            .enumerate()
            .find(|(_index, &character)| character == b'\0')
            .expect("Buffer must contain terminating zero.")
            .0;
        let c_str = unsafe { CStr::from_bytes_with_nul_unchecked(&self.buffer[..=end]) };
        c_str.to_string_lossy().into_owned()
    }

    /// Length in characters including space for terminating zero
    pub fn len_buf(&self) -> usize {
        self.buffer.len()
    }

    pub fn mut_ptr(&mut self) -> *mut SqlChar {
        mut_buf_ptr(&mut self.buffer)
    }
}

/// We use this as an output buffer for strings. Allows for detecting truncation.
pub struct OutputStringBuffer {
    /// Buffer holding the string. Must also contains space for a terminating zero.
    buffer: SzBuffer,
    /// After the buffer has been filled, this should contain the actual length of the string. Can
    /// be used to detect truncation.
    actual_length: i16,
}

impl OutputStringBuffer {
    /// Creates a new instance of an output string buffer which can hold strings up to a size of
    /// `max_str_len` characters.
    pub fn with_buffer_size(max_str_len: usize) -> Self {
        Self {
            buffer: SzBuffer::with_capacity(max_str_len),
            actual_length: 0,
        }
    }

    /// Ptr to the internal buffer. Used by ODBC API calls to fill the buffer.
    pub fn mut_buf_ptr(&mut self) -> *mut SqlChar {
        self.buffer.mut_ptr()
    }

    /// Length of the internal buffer in characters including the terminating zero.
    pub fn buf_len(&self) -> i16 {
        // Since buffer must always be able to hold at least one element, substracting `1` is always
        // defined
        self.buffer.len_buf().try_into().unwrap()
    }

    /// Mutable pointer to actual output string length. Used by ODBC API calls to report truncation.
    pub fn mut_actual_len_ptr(&mut self) -> *mut i16 {
        &mut self.actual_length as *mut i16
    }

    /// Call this method to extract string from buffer after ODBC has filled it.
    pub fn to_utf8(&self) -> String {
        self.buffer.to_utf8()
    }

    /// True if the buffer had not been large enough to hold the string.
    pub fn is_truncated(&self) -> bool {
        self.actual_length >= self.buffer.len_buf().try_into().unwrap()
    }
}

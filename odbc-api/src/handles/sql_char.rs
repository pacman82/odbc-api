//! The idea is to handle most of the conditional compilation around different SQL character types
//! in this module, so the rest of the crate doesn't have to.

// The rather akward expression:
// `#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]` is used to
// annotate things which should only compile if we use UTF-16 to communicate to the data source.
// We use its negation:
// `#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]` to
// indicate a "narrow" charset for communicating with the datasource, which we assume to be UTF-8
//
// Currently I did not find a better way to use narrow function on non-windows platforms and wide
// functions on windows platforms by default. I also want to enable explicitly overwriting the
// default on both platforms. See also the documentation of the `narrow` and `wide` features in the
// Cargo.toml manifest.

use super::buffer::{buf_ptr, mut_buf_ptr};
use std::{
    borrow::Cow,
    mem::{size_of, size_of_val},
};

#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
use std::{ffi::CStr, string::FromUtf8Error};

#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
use std::{
    char::{decode_utf16, DecodeUtf16Error},
    marker::PhantomData,
};

#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
use widestring::{U16CStr, U16String};

#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
pub type SqlChar = u8;
#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
pub type SqlChar = u16;

#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
pub type DecodingError = FromUtf8Error;
#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
pub type DecodingError = DecodeUtf16Error;

#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
pub fn slice_to_utf8(text: &[u8]) -> Result<String, FromUtf8Error> {
    String::from_utf8(text.to_owned())
}
#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
pub fn slice_to_utf8(text: &[u16]) -> Result<String, DecodeUtf16Error> {
    decode_utf16(text.iter().copied()).collect()
}

#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
pub fn slice_to_cow_utf8(text: &[u8]) -> Cow<str> {
    String::from_utf8_lossy(text)
}
#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
pub fn slice_to_cow_utf8(text: &[u16]) -> Cow<str> {
    let text: Result<String, _> = decode_utf16(text.iter().copied()).collect();
    text.unwrap().into()
}

#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
fn sz_to_utf8(buffer: &[u16]) -> String {
    let c_str = U16CStr::from_slice_truncate(buffer).unwrap();
    c_str.to_string_lossy()
}
#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
fn sz_to_utf8(buffer: &[u8]) -> String {
    // Truncate slice at first zero.
    let end = buffer
        .iter()
        .enumerate()
        .find(|(_index, &character)| character == b'\0')
        .expect("Buffer must contain terminating zero.")
        .0;
    let c_str = unsafe { CStr::from_bytes_with_nul_unchecked(&buffer[..=end]) };
    c_str.to_string_lossy().into_owned()
}

/// Buffer length in bytes, not characters
pub fn binary_length(buffer: &[SqlChar]) -> usize {
    size_of_val(buffer)
}

/// `true` if the buffer has not been large enough to hold the entire string.
///
/// # Parameters
///
/// - `actuel_length_bin`: Actual length in bytes, but excluding the terminating zero.
pub fn is_truncated_bin(buffer: &[SqlChar], actual_length_bin: usize) -> bool {
    size_of_val(buffer) <= actual_length_bin
}

/// Resizes the underlying buffer to fit the size required to hold the entire string including
/// terminating zero. Required length is provided in bytes (not characters), excluding the
/// terminating zero.
pub fn resize_to_fit_with_tz(buffer: &mut Vec<SqlChar>, required_binary_length: usize) {
    // In order to use only minimal memory for drivers which stick to the ODBC standard we would
    // use `+1` in the statement beneath. However it turns out the PostgreSQL driver will fill the
    // last value with `0` instead of the last latter when used with a wide `SqlChar`. So we use
    // `+2` to make it work with PostgreSql on windows, too.
    buffer.resize((required_binary_length / size_of::<SqlChar>()) + 2, 0);
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
    #[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
    text: U16String,
    /// We include the lifetime in the declaration of the type still, so the borrow checker
    /// complains, if we would mess up the compilation for narrow methods.
    #[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
    _ref: PhantomData<&'a str>,
    /// In the case of narrow compiliation we just forward the string silce unchanged
    #[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
    text: &'a str,
}

impl<'a> SqlText<'a> {
    #[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
    /// Create an SqlText buffer from an UTF-8 string slice
    pub fn new(text: &'a str) -> Self {
        Self {
            text: U16String::from_str(text),
            _ref: PhantomData,
        }
    }
    #[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
    /// Create an SqlText buffer from an UTF-8 string slice
    pub fn new(text: &'a str) -> Self {
        Self { text }
    }

    #[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
    pub fn ptr(&self) -> *const u16 {
        buf_ptr(self.text.as_slice())
    }
    #[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
    pub fn ptr(&self) -> *const u8 {
        buf_ptr(self.text.as_bytes())
    }

    /// Length in characters
    pub fn len_char(&self) -> usize {
        self.text.len()
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

    /// Create an owned utf-8 string from the internal buffer representation.
    pub fn to_utf8(&self) -> String {
        sz_to_utf8(&self.buffer)
    }
}

/// We use this as an output buffer for strings. Allows for detecting truncation.
pub struct OutputStringBuffer {
    /// Buffer holding the string. Must also contains space for a terminating zero.
    buffer: Vec<SqlChar>,
    /// After the buffer has been filled, this should contain the actual length of the string. Can
    /// be used to detect truncation.
    actual_length: i16,
}

impl OutputStringBuffer {
    /// Creates an empty string buffer. Useful if you want to e.g. use a prompt to complete the
    /// connection string, but are not interested in the actual completed connection string.
    pub fn empty() -> Self {
        Self::with_buffer_size(0)
    }

    /// Creates a new instance of an output string buffer which can hold strings up to a size of
    /// `max_str_len - 1` characters. `-1 because one place is needed for the terminating zero.
    /// To hold a connection string the size should be at least 1024.
    pub fn with_buffer_size(max_str_len: usize) -> Self {
        Self {
            buffer: vec![0; max_str_len],
            actual_length: 0,
        }
    }

    /// Ptr to the internal buffer. Used by ODBC API calls to fill the buffer.
    pub fn mut_buf_ptr(&mut self) -> *mut SqlChar {
        mut_buf_ptr(&mut self.buffer)
    }

    /// Length of the internal buffer in characters including the terminating zero.
    pub fn buf_len(&self) -> i16 {
        // Since buffer must always be able to hold at least one element, substracting `1` is always
        // defined
        self.buffer.len().try_into().unwrap()
    }

    /// Mutable pointer to actual output string length. Used by ODBC API calls to report truncation.
    pub fn mut_actual_len_ptr(&mut self) -> *mut i16 {
        &mut self.actual_length as *mut i16
    }

    /// Call this method to extract string from buffer after ODBC has filled it.
    pub fn to_utf8(&self) -> String {
        if self.buffer.is_empty() {
            return String::new();
        }

        if self.is_truncated() {
            // If the string is truncated we return the entire buffer excluding the terminating
            // zero.
            slice_to_utf8(&self.buffer[0..(self.buffer.len() - 1)]).unwrap()
        } else {
            // If the string is not truncated, we return not the entire buffer, but only the slice
            // containing the actual string.
            let actual_length: usize = self.actual_length.try_into().unwrap();
            slice_to_utf8(&self.buffer[0..actual_length]).unwrap()
        }
    }

    /// True if the buffer had not been large enough to hold the string.
    pub fn is_truncated(&self) -> bool {
        self.actual_length >= self.buffer.len().try_into().unwrap()
    }
}

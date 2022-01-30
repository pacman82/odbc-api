//! The idea is to handle most of the conditional compilation around different SQL character types
//! in this module, so the rest of the crate doesn't have to.

use super::buffer::buf_ptr;

#[cfg(feature = "narrow")]
use std::ffi::CStr;

#[cfg(not(feature = "narrow"))]
use std::marker::PhantomData;

#[cfg(not(feature = "narrow"))]
use widestring::{U16CStr, U16String};

#[cfg(feature = "narrow")]
pub type SqlChar = u8;
#[cfg(not(feature = "narrow"))]
pub type SqlChar = u16;

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

    pub fn len(&self) -> i16 {
        self.text.len().try_into().unwrap()
    }

    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }
}

/// Use this buffer type to fetch zero terminated strings from the ODBC API. Either allocates a
/// buffer for wide or narrow strings dependend on the features set.
pub struct SzBuffer {
    pub buffer: Vec<SqlChar>,
}

impl SzBuffer {
    /// Creates a buffer which can hold at least `capacity` characters, excluding the terminating
    /// zero. Or phrased differently. It will allocate one additional character to hold the
    /// terminating zero, so the caller should not factor it into the size of capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: vec![0; capacity],
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
        let end = self.buffer
            .iter()
            .enumerate()
            .find(|(_index, &character)| character == b'\0')
            .expect("Buffer must contain terminating zero.")
            .0;
        let c_str = unsafe { CStr::from_bytes_with_nul_unchecked(&self.buffer[..=end]) };
        c_str.to_string_lossy().into_owned()
    }
}

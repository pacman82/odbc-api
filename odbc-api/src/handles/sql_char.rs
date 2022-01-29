use super::buffer::buf_ptr;

#[cfg(feature = "narrow")]
use std::ffi::CStr;

#[cfg(not(feature = "narrow"))]
use widestring::{U16CStr, U16Str, U16String};

#[cfg(feature = "narrow")]
pub type SqlChar = u8;
#[cfg(not(feature = "narrow"))]
pub type SqlChar = u16;

#[cfg(feature = "narrow")]
pub type SqlStr = str;
#[cfg(not(feature = "narrow"))]
pub type SqlStr = U16Str;


#[cfg(not(feature = "narrow"))]
/// Truncate the input slice at the first 'nul' and return everything up to then as a string slice.
pub fn truncate_slice_to_sql_c_str(slice: &[u16]) -> &U16CStr {
    U16CStr::from_slice_truncate(slice).unwrap()
}
#[cfg(feature = "narrow")]
/// Truncate the input slice at the first 'nul' and return everything up to then as a string slice.
pub fn truncate_slice_to_sql_c_str(slice: &[u8]) -> &CStr {
    let end = slice
        .iter()
        .enumerate()
        .find(|(_index, &character)| character == b'\0')
        .expect("Buffer must contain terminating zero.")
        .0;
    unsafe { CStr::from_bytes_with_nul_unchecked(&slice[..=end]) }
}


#[cfg(feature = "narrow")]
/// Take ownership of the CStr as String
pub fn c_str_to_string(c_str: &CStr) -> String {
    c_str.to_string_lossy().into_owned()
}
#[cfg(not(feature = "narrow"))]
/// Take ownership of the CStr as String
pub fn c_str_to_string(c_str: &U16CStr) -> String {
    c_str.to_string_lossy()
}

/// This is a no op for narrow strings, as we assume them to be UTF-8
#[cfg(feature = "narrow")]
pub fn to_sql_text(text: &str) -> &str {
    text
}
/// Convert a narrow UTF-8 string into a wide UTF-16 string.
#[cfg(not(feature = "narrow"))]
pub fn to_sql_text(text: &str) -> U16String {
    U16String::from_str(text)
}

/// This is a no op for narrow strings, as we assume them to be UTF-8
#[cfg(feature = "narrow")]
pub fn text_ptr(text: &str) -> * const u8 {
    buf_ptr(text.as_bytes())
}
/// Convert a narrow UTF-8 string into a wide UTF-16 string.
#[cfg(not(feature = "narrow"))]
pub fn text_ptr(text: &U16Str) -> * const u16 {
    buf_ptr(text.as_slice())
}

#[cfg(feature = "narrow")]
pub fn text_ref<'a>(text: &&'a str) -> &'a str {
    text
}
#[cfg(not(feature = "narrow"))]
pub fn text_ref(text: &U16Str) -> &U16Str {
    text
}
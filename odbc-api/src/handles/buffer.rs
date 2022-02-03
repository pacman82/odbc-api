use std::{
    cmp::min,
    ptr::{null, null_mut},
};

/// Clamps a usize between `0` and `i16::MAX`.
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

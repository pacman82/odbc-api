use odbc_sys::SmallInt;
use std::{cmp::min, ptr::null_mut};

/// Clamps a usize between `0` and `SmallInt::MAX`.
pub fn clamp_small_int(n: usize) -> SmallInt {
    min(n, SmallInt::MAX as usize) as SmallInt
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

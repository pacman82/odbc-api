use odbc_sys::{CDataType, SmallInt, Pointer, Len};
use std::{
    cmp::min,
    ptr::{null, null_mut},
};

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

/// Returns a pointer suitable to be passed as an output buffer to ODBC functions. Most notably it
/// will return NULL for empty buffers.
pub fn buf_ptr<T>(buffer: &[T]) -> *const T {
    if buffer.is_empty() {
        null()
    } else {
        buffer.as_ptr()
    }
}

/// A buffer suitable to bind to a cursor using `bind_col`.
pub unsafe trait Buffer {
    fn target_type(&self) -> CDataType;
    fn target_value(&self) -> Pointer;
    fn buffer_length(&self) -> Len;
}
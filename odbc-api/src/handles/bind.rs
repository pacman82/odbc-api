//! Implementation and types required to bind arguments to ODBC parameters.

use crate::DataType;
use odbc_sys::{CDataType, Len};
use std::{ffi::c_void};

/// A type which has a layout suitable to be bound to ODBC.
pub unsafe trait CData {
    /// The CDataType corresponding with this types layout.
    fn cdata_type(&self) -> CDataType;

    /// Indicates the length of variable sized types. May be zero for fixed sized types.
    fn indicator_ptr(&self) -> *const Len;

    /// Pointer to a value corresponding to the one described by `cdata_type`.
    fn value_ptr(&self) -> *const c_void;
}

/// Can be bound to a single placeholder in an SQL statement.
///
/// Users usually won't utilize this trait directly.
pub unsafe trait Input: CData {
    fn data_type(&self) -> DataType;
}

/// A type which can be used to receive Data from ODBC.
///
/// Users usually won't utilize this trait directly.
pub unsafe trait InputArray: Input {
    /// Maximum length of the type. It is required to index values in bound buffers, if more than
    /// one parameter is bound.
    fn buffer_length(&self) -> Len;
}


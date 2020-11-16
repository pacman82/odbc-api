//! Implementation and types required to bind arguments to ODBC parameters.

use crate::DataType;
use odbc_sys::{CDataType, Len};
use std::ffi::c_void;

/// Provides description of C type layout and pointers to it. Used to bind and buffers to ODBC
/// statements.
pub unsafe trait CData {
    /// The identifier of the C data type of the value buffer. When it is retrieving data from the
    /// data source with `fetch`, the driver converts the data to this type. When it sends data to
    /// the source, the driver converts the data from this type.
    fn cdata_type(&self) -> CDataType;

    /// Indicates the length of variable sized types. May be zero for fixed sized types.
    fn indicator_ptr(&self) -> *const Len;

    /// Pointer to a value corresponding to the one described by `cdata_type`.
    fn value_ptr(&self) -> *const c_void;

    /// Maximum length of the type. It is required to index values in bound buffers, if more than
    /// one parameter is bound. Can be set to zero for types not bound as parameter arrays, i.e.
    /// `CStr`.
    fn buffer_length(&self) -> Len;
}

/// Can be bound to a single placeholder in an SQL statement.
///
/// Users usually won't utilize this trait directly.
pub unsafe trait Input: CData {

    /// The SQL data as which the parameter is bound to ODBC.
    fn data_type(&self) -> DataType;
}

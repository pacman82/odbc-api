//! Implementation and types required to bind arguments to ODBC parameters.

use odbc_sys::CDataType;
use std::ffi::c_void;

use crate::DataType;

/// Provides description of C type layout and pointers to it. Used to bind and buffers to ODBC
/// statements.
///
/// # Safety
///
/// In case of variable sized types [`Self::indicator_ptr`] must not exceed the value pointed to by
/// [`Self::value_ptr`]. This requirement is a bit tricky since, if the same indicator buffer is
/// used in an output paramater the indicator value may be larger in case of truncation.
pub unsafe trait CData {
    /// The identifier of the C data type of the value buffer. When it is retrieving data from the
    /// data source with `fetch`, the driver converts the data to this type. When it sends data to
    /// the source, the driver converts the data from this type.
    fn cdata_type(&self) -> CDataType;

    /// Indicates the length of variable sized types. May be zero for fixed sized types. Used to
    /// determine the size or existence of input parameters.
    fn indicator_ptr(&self) -> *const isize;

    /// Pointer to a value corresponding to the one described by `cdata_type`.
    fn value_ptr(&self) -> *const c_void;

    /// Maximum length of the type in bytes (not characters). It is required to index values in
    /// bound buffers, if more than one parameter is bound. Can be set to zero for types not bound
    /// as parameter arrays, i.e. `CStr`.
    fn buffer_length(&self) -> isize;
}

/// A type which can be bound mutably to ODBC.
///
/// # Safety
///
/// Care must be taken to only implement this for types which can maintain their invariants then
/// ODBC writes into them.
pub unsafe trait CDataMut: CData {
    /// Indicates the length of variable sized types. May be zero for fixed sized types.
    fn mut_indicator_ptr(&mut self) -> *mut isize;

    /// Pointer to a value corresponding to the one described by `cdata_type`.
    fn mut_value_ptr(&mut self) -> *mut c_void;
}

/// Stream which can be bound as in input parameter to a statement in order to provide the actual
/// data at statement execution time, rather than preallocated buffers.
///
/// # Safety
///
/// [`Self::stream_ptr`] must return a valid pointer to a reference of a dynamic Blob trait object
/// `(*mut &mut dyn Blob)` which must at least be valid for the lifetime of the instance. The
/// indicator pointer and C data type must describe that instance truthfully.
pub unsafe trait DelayedInput {
    /// Then streaming data to the "data source" the driver converts the data from this type.
    fn cdata_type(&self) -> CDataType;

    /// Either [`odbc_sys::DATA_AT_EXEC`] in case of streaming from a stream of unknown length (e.g.
    /// stdin) or the result of [`odbc_sys::len_data_at_exec`] if the length of the stream is known
    /// in advance (e.g. a File).
    fn indicator_ptr(&self) -> *const isize;

    /// Pointer to reference of [`crate::parameter::Blob`] the stream or an application defined
    /// value identifying the stream.
    fn stream_ptr(&mut self) -> *mut c_void;
}

/// Can be bound to a single placeholder in an SQL statement.
///
/// Users usually will not utilize this trait directly.
pub trait HasDataType {
    /// The SQL data as which the parameter is bound to ODBC.
    fn data_type(&self) -> DataType;
}

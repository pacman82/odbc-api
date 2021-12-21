use odbc_sys::{Handle, HandleType};

/// Provides access to the raw underlying ODBC handle.
///
/// # Safety
///
/// The handle provided by `as_handle` must be valid and match the type returned by `handle_type`.
pub unsafe trait AsHandle {
    /// The raw underlying ODBC handle used to talk to the ODBC C API. The handle must be valid.
    fn as_handle(&self) -> Handle;

    /// The type of the ODBC handle returned by `as_handle`. This is a method rather than a constant
    /// in order to make the type object safe.
    fn handle_type(&self) -> HandleType;
}

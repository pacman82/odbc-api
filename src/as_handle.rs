use odbc_sys::{Handle, HandleType};

/// Provides access to the raw underlying ODBC handle.
pub unsafe trait AsHandle {
    /// The raw underlying ODBC handle used to talk to the ODBC C API. The handle must be valid.
    fn as_handle(&self) -> Handle;

    /// The type of the ODBC handle returned by `as_handle`.
    fn handle_type(&self) -> HandleType;
}

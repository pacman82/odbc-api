use odbc_sys::{SQLFreeHandle, HDbc, Handle, HandleType, SqlReturn};
use std::thread::panicking;

/// Wraps a valid ODBC Connection handle.
pub struct Connection {
    handle: HDbc
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe {
            match SQLFreeHandle(HandleType::Dbc, self.handle as Handle) {
                SqlReturn::SUCCESS => (),
                other => {
                    // Avoid panicking, if we already have a panic. We don't want to mask the
                    // original error.
                    if !panicking() {
                        panic!("Unexepected return value of SQLFreeHandle: {:?}", other)
                    }
                }
            }
        }
    }
}

impl Connection {
    /// # Safety
    ///
    /// Call this method only with a valid (successfully allocated) ODBC connection handle.
    pub unsafe fn new(handle: HDbc) -> Self {
        Self { handle }
    }
}

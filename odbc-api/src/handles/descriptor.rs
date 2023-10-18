use std::marker::PhantomData;

use odbc_sys::{HDesc, HStmt, Handle, HandleType};

use super::AsHandle;

/// A descriptor associated with a statement. This wrapper does not wrap explicitly allocated
/// descriptors which have the connection as parent, but usually implicitly allocated ones
/// associated with the statement. It could also represent an explicitly allocated one, but ony in
/// the context there it is associated with a statement and currently borrowed from it.
///
/// * IPD Implementation parameter descriptor
/// * APD Application parameter descriptor
/// * IRD Implemenation row descriptor
/// * ARD Application row descriptor
pub struct Descriptor<'stmt> {
    handle: HDesc,
    parent: PhantomData<&'stmt HStmt>,
}

impl<'stmt> Descriptor<'stmt> {
    /// # Safety
    ///
    /// Call this method only with a valid (successfully allocated) ODBC descriptor handle.
    pub unsafe fn new(handle: HDesc) -> Self {
        Self {
            handle,
            parent: PhantomData,
        }
    }

    /// Directly acces the underlying ODBC handle.
    pub fn as_sys(&self) -> HDesc {
        self.handle
    }
}

unsafe impl<'stmt> AsHandle for Descriptor<'stmt> {
    fn as_handle(&self) -> Handle {
        self.handle as Handle
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Desc
    }
}

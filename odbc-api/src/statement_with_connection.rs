use odbc_sys::{HStmt, Handle, HandleType};

use crate::{
    handles::{drop_handle, AsHandle, Statement},
    Connection,
};

/// Statement handle which also takes ownership of Connection
pub struct StatementWithConnection<'env> {
    handle: HStmt,
    _parent: Connection<'env>,
}

impl<'env> StatementWithConnection<'env> {
    pub unsafe fn new(handle: HStmt, parent: Connection<'env>) -> Self {
        Self {
            _parent: parent,
            handle,
        }
    }
}

impl<'s> Drop for StatementWithConnection<'s> {
    fn drop(&mut self) {
        unsafe {
            drop_handle(self.handle as Handle, HandleType::Stmt);
        }
    }
}

unsafe impl AsHandle for StatementWithConnection<'_> {
    fn as_handle(&self) -> Handle {
        self.handle as Handle
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Stmt
    }
}

impl Statement for StatementWithConnection<'_> {
    fn as_sys(&self) -> HStmt {
        self.handle
    }
}

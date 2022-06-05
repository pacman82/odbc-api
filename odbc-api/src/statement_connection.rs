use odbc_sys::{HStmt, Handle, HandleType};

use crate::{
    handles::{drop_handle, AsHandle, AsStatementRef, Statement, StatementRef},
    Connection,
};

/// Statement handle which also takes ownership of Connection
pub struct StatementConnection<'env> {
    handle: HStmt,
    _parent: Connection<'env>,
}

impl<'env> StatementConnection<'env> {
    pub(crate) unsafe fn new(handle: HStmt, parent: Connection<'env>) -> Self {
        Self {
            _parent: parent,
            handle,
        }
    }

    pub fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        unsafe { StatementRef::new(self.handle) }
    }
}

impl<'s> Drop for StatementConnection<'s> {
    fn drop(&mut self) {
        unsafe {
            drop_handle(self.handle as Handle, HandleType::Stmt);
        }
    }
}

unsafe impl AsHandle for StatementConnection<'_> {
    fn as_handle(&self) -> Handle {
        self.handle as Handle
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Stmt
    }
}

impl Statement for StatementConnection<'_> {
    fn as_sys(&self) -> HStmt {
        self.handle
    }
}

impl<'o> AsStatementRef for StatementConnection<'o> {
    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        self.as_stmt_ref()
    }
}

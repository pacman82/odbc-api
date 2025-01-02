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

impl Drop for StatementConnection<'_> {
    fn drop(&mut self) {
        unsafe {
            drop_handle(self.handle as Handle, HandleType::Stmt);
        }
    }
}

/// According to the ODBC documentation this is safe. See:
/// <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/multithreading>
///
/// Operations to a statement imply that interior state of the connection might be mutated,
/// depending on the implementation detail of the ODBC driver. According to the ODBC documentation
/// this could always be considered save, since Connection handles are basically described as
/// `Sync`. Yet making connections `Send` could very well lead to different statements on different
/// threads actually changing the connection at the same time and truly relying on the thread safety
/// of the ODBC driver. I am sceptical. Especially if on Linux unixODBC would not be configured to
/// protect connections.
///
/// Note to users of `unixodbc`: You may configure the threading level to make unixodbc
/// synchronize access to the driver (and thereby making them thread safe if they are not thread
/// safe by themself. This may however hurt your performance if the driver would actually be able to
/// perform operations in parallel.
///
/// See: <https://stackoverflow.com/questions/4207458/using-unixodbc-in-a-multithreaded-concurrent-setting>
///
/// `StatementConnection` however also owns the connection exclusively. Since connections are `Send`
/// it is reasonable to assume this would work even if implementers of the ODBC driver do not care
/// in particular about thread safety.
unsafe impl Send for StatementConnection<'_> {}

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

impl AsStatementRef for StatementConnection<'_> {
    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        self.as_stmt_ref()
    }
}

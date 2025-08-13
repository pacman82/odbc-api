use odbc_sys::{HStmt, Handle, HandleType};

use crate::handles::{AnyHandle, AsStatementRef, Statement, StatementRef, drop_handle};

/// Statement handle which also takes ownership of Connection
#[derive(Debug)]
pub struct StatementConnection<C> {
    handle: HStmt,
    /// We do not do anything with the parent, besides keeping it alive.
    _parent: C,
}

impl<C> StatementConnection<C>
where
    C: ConnectionOwner,
{
    /// # Safety
    /// 
    /// Handle must be a valid statement handle and the parent connection must be valid for the
    /// lifetime of parent.
    pub(crate) unsafe fn new(handle: HStmt, parent: C) -> Self {
        Self {
            _parent: parent,
            handle,
        }
    }

    pub fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        unsafe { StatementRef::new(self.handle) }
    }
}

impl<C> Drop for StatementConnection<C> {
    fn drop(&mut self) {
        unsafe {
            drop_handle(self.handle as Handle, HandleType::Stmt);
        }
    }
}

/// Implementers of this trait are guaranteed to keep a connection alive and in connected state for
/// the lifetime of the instance.
///
/// E.g. [`super::Connection`] is not a `ConnectionOwner`, since it is not guaranteed to keep the
/// connection alive. Nor would it close the connection at the end of the lifetime on Drop.
///
/// # Safety
///
/// Instance must keep the connection it owns alive and open.
pub unsafe trait ConnectionOwner {}

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
unsafe impl<C> Send for StatementConnection<C> where C: Send {}

unsafe impl<C> AnyHandle for StatementConnection<C>
where
    C: ConnectionOwner,
{
    fn as_handle(&self) -> Handle {
        self.handle as Handle
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Stmt
    }
}

impl<C> Statement for StatementConnection<C>
where
    C: ConnectionOwner,
{
    fn as_sys(&self) -> HStmt {
        self.handle
    }
}

impl<C> AsStatementRef for StatementConnection<C>
where
    C: ConnectionOwner,
{
    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        self.as_stmt_ref()
    }
}
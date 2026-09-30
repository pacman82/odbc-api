use odbc_sys::{HStmt, Handle, HandleType};

use crate::{handles::{AnyHandle, SqlResult, Statement, StatementRef, drop_handle}};

/// Statement handle which also takes ownership of Connection
#[derive(Debug)]
pub struct StatementConnection<C> {
    handle: HStmt,
    /// We keep the parent connection alive. It must not be used for any action which could
    /// invalidate the statement handle. 
    parent: C,
}

impl<C> StatementConnection<C>
where
    C: StatementParent,
{
    /// # Safety
    ///
    /// Handle must be a valid statement handle and the parent connection must be valid for the
    /// lifetime of parent.
    pub(crate) unsafe fn new(handle: HStmt, parent: C) -> Self {
        Self {
            parent,
            handle,
        }
    }

    pub fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        unsafe { StatementRef::new(self.handle) }
    }

    /// Access the parent connection.
    /// 
    /// # Safety
    /// 
    /// The parent must not be used for anything which could invalidate the statement handle.
    pub unsafe fn parent(&self) -> &C {
        &self.parent
    }
}

impl<C> Drop for StatementConnection<C> {
    fn drop(&mut self) {
        unsafe {
            drop_handle(self.handle.as_handle(), HandleType::Stmt);
        }
    }
}

/// Implementers of this trait are guaranteed to keep a connection alive and in connected state for
/// the lifetime of the instance.
///
/// E.g. [`super::Connection`] is not a [`StatementParent`], since it is not guaranteed to keep the
/// connection alive. Nor would it close the connection at the end of the lifetime on Drop.
///
/// # Safety
///
/// Instance must keep the connection it owns alive and open.
pub unsafe trait StatementParent {}

/// According to the ODBC documentation this is safe. See:
/// <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/multithreading>
///
/// Operations to a statement imply that interior state of the connection might be mutated,
/// depending on the implementation detail of the ODBC driver. According to the ODBC documentation
/// this could always be considered save.
unsafe impl<C> Send for StatementConnection<C> where C: StatementParent {}

unsafe impl<C> AnyHandle for StatementConnection<C>
where
    C: StatementParent,
{
    fn as_handle(&self) -> Handle {
        self.handle.as_handle()
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Stmt
    }
}

impl<C> Statement for StatementConnection<C>
where
    C: StatementParent,
{
    fn as_sys(&self) -> HStmt {
        self.handle
    }

    fn end_cursor_scope(&mut self) -> SqlResult<()> {
        SqlResult::Success(())
    }
}


use crate::{buffer::buf_ptr, error::ToResult, AsHandle, Error};
use odbc_sys::{
    HDbc, HStmt, Handle, HandleType, SQLCloseCursor, SQLExecDirectW, SQLFreeHandle, SqlReturn,
};
use std::{convert::TryInto, marker::PhantomData, thread::panicking};
use widestring::U16Str;

pub struct Statement<'s> {
    parent: PhantomData<&'s HDbc>,
    handle: HStmt,
}

unsafe impl<'c> AsHandle for Statement<'c> {
    fn as_handle(&self) -> Handle {
        self.handle as Handle
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Stmt
    }
}

impl<'s> Drop for Statement<'s> {
    fn drop(&mut self) {
        unsafe {
            match SQLFreeHandle(HandleType::Stmt, self.handle as Handle) {
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

impl<'s> Statement<'s> {
    /// # Safety
    ///
    /// `handle` must be a valid (successfully allocated) statement handle.
    pub unsafe fn new(handle: HStmt) -> Self {
        Self {
            handle,
            parent: PhantomData,
        }
    }

    /// Executes a preparable statement, using the current values of the parameter marker variables
    /// if any parameters exist in the statement. SQLExecDirect is the fastest way to submit an SQL
    /// statement for one-time execution.
    ///
    /// # Return
    ///
    /// Returns `true` if a cursor is created. If `false` is returned no cursor has been created (
    /// e.g. the query came back empty). Note that an empty query may also create a cursor with zero
    /// rows. If a cursor is created. `close_cursor` should be called after it is no longer used.
    pub fn exec_direct(&mut self, statement_text: &U16Str) -> Result<bool, Error> {
        unsafe {
            match SQLExecDirectW(
                self.handle,
                buf_ptr(statement_text.as_slice()),
                statement_text.len().try_into().unwrap(),
            ) {
                SqlReturn::NO_DATA => Ok(false),
                other => other.to_result(self).map(|()| true),
            }
        }
    }

    pub fn close_cursor(&mut self) -> Result<(), Error> {
        unsafe { SQLCloseCursor(self.handle) }.to_result(self)
    }
}

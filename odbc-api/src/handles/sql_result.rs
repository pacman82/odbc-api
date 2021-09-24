use crate::Error;

use super::{
    as_handle::AsHandle, diagnostics::Record as DiagnosticRecord, logging::log_diagnostics,
};
use odbc_sys::SqlReturn;

/// Result of an ODBC function call. Variants hold the same meaning as the constants associated with
/// [`SqlReturn`]. This type may hold results, but it is still the responsibility of the user to
/// fetch and handle the diagnostics in case of an Error.
#[derive(Debug)]
pub enum SqlResult<T> {
    /// The function has been executed successfully.
    Success(T),
    /// The function has been executed successfully. There have been warnings.
    SuccessWithInfo(T),
    /// Function returned error state. Check diagnostics.
    Error {
        /// Name of the ODBC Api call which caused the error. This might help interpreting
        /// associatedif the error ODBC diagnostics if the error is bubbeld all the way up to the
        /// end users output, but the context is lost.
        function: &'static str,
    },
}

impl SqlResult<()> {
    /// Append a return value a successful to Result
    pub fn on_success<F, T>(self, f: F) -> SqlResult<T>
    where
        F: FnOnce() -> T,
    {
        self.map(|()| f())
    }
}

impl<T> SqlResult<T> {
    /// Logs diagonstics of `handle` if variant is either `Error` or `SuccessWithInfo`.
    pub fn log_diagnostics(&self, handle: &dyn AsHandle) {
        match self {
            SqlResult::Error { .. } | SqlResult::SuccessWithInfo(_) => log_diagnostics(handle),
            _ => (),
        }
    }

    pub fn into_result(self, handle: &dyn AsHandle) -> Result<T, Error> {
        match self {
            // The function has been executed successfully. Holds result.
            SqlResult::Success(value) => Ok(value),
            // The function has been executed successfully. There have been warnings. Holds result.
            SqlResult::SuccessWithInfo(value) => {
                log_diagnostics(handle);
                Ok(value)
            }
            SqlResult::Error { function } => {
                let mut record = DiagnosticRecord::default();
                if record.fill_from(handle, 1) {
                    log_diagnostics(handle);
                    Err(Error::Diagnostics { record, function })
                } else {
                    Err(Error::NoDiagnostics)
                }
            }
        }
    }

    /// `True` if variant is [`SqlResult::Error`].
    pub fn is_err(&self) -> bool {
        matches!(self, SqlResult::Error { .. })
    }

    /// Applies `f` to any value wrapped in `Success` or `SuccessWithInfo`.
    pub fn map<U, F>(self, f: F) -> SqlResult<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            SqlResult::Success(v) => SqlResult::Success(f(v)),
            SqlResult::SuccessWithInfo(v) => SqlResult::SuccessWithInfo(f(v)),
            SqlResult::Error { function } => SqlResult::Error { function },
        }
    }

    pub fn unwrap(self) -> T {
        match self {
            SqlResult::Success(v) | SqlResult::SuccessWithInfo(v) => v,
            SqlResult::Error { .. } => panic!("Unwraping SqlResult::Error"),
        }
    }
}

pub trait ExtSqlReturn {
    fn into_sql_result(self, function_name: &'static str) -> SqlResult<()>;

    /// Translates NO_DATA to None
    fn into_opt_sql_result(self, function_name: &'static str) -> Option<SqlResult<()>>;
}

impl ExtSqlReturn for SqlReturn {
    fn into_sql_result(self, function: &'static str) -> SqlResult<()> {
        match self {
            SqlReturn::SUCCESS => SqlResult::Success(()),
            SqlReturn::SUCCESS_WITH_INFO => SqlResult::SuccessWithInfo(()),
            SqlReturn::ERROR => SqlResult::Error { function },
            r => panic!(
                "Unexpected return value '{:?}' for ODBC function '{}'",
                r, function
            ),
        }
    }

    fn into_opt_sql_result(self, function: &'static str) -> Option<SqlResult<()>> {
        match self {
            SqlReturn::NO_DATA => None,
            other => Some(other.into_sql_result(function)),
        }
    }
}

pub trait IntoResult {
    fn into_result(self, handle: &dyn AsHandle, function: &'static str) -> Result<(), Error>;
}

impl IntoResult for SqlReturn {
    fn into_result(
        self: SqlReturn,
        handle: &dyn AsHandle,
        function: &'static str,
    ) -> Result<(), Error> {
        self.into_sql_result(function).into_result(handle)
    }
}

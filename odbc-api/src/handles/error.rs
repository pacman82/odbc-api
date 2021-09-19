use std::io;

use super::{
    as_handle::AsHandle, diagnostics::Record as DiagnosticRecord, logging::log_diagnostics,
};
use odbc_sys::SqlReturn;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
/// Error type used to indicate a low level ODBC call returned with SQL_ERROR.
pub enum Error {
    /// No Diagnostics available. This is usually the case if allocation of the ODBC Environment
    /// itself fails. In that case no object exist to obtain the diagnostic record from.
    #[error("No Diagnostics available.")]
    NoDiagnostics,
    /// SQL Error had been returned by a low level ODBC function call. A Diagnostic record is
    /// obtained and associated with this error.
    #[error("ODBC emitted an error calling '{function}':\n{record}")]
    Diagnostics {
        /// Diagnostic record returned by the ODBC driver manager
        record: DiagnosticRecord,
        /// ODBC API call which produced the diagnostic record
        function: &'static str,
    },
    /// A user dialog to complete the connection string has been aborted.
    #[error("The dialog shown to provide or complete the connection string has been aborted.")]
    AbortedConnectionStringCompletion,
    /// An error returned if we fail to set the ODBC version
    #[error(
        "ODBC diver manager does not seem to support the required ODBC version 3.80. (Most \
        likely you need to update unixODBC if you run on a Linux. Diagnostic record returned by \
        SQLSetEnvAttr:\n{0}"
    )]
    UnsupportedOdbcApiVersion(DiagnosticRecord),
    /// An error emitted by an `std::io::ReadBuf` implementation used as an input argument.
    #[error("Sending data to the database at statement execution time failed. IO error:\n{0}")]
    FailedReadingInput(io::Error),
    /// Driver returned "invalid attribute" then setting the row array size. Most likely the array
    /// size is to large. Instead of returing "option value changed (SQLSTATE 01S02)" like suggested
    /// in <https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetstmtattr-function> the
    /// driver returned an error instead.
    #[error(
        "An invalid row array size (aka. batch size) has been set. The ODBC drivers should just \
        emit a warning and emmit smaller batches, but not all do (yours does not at least). Try \
        fetching data from the database in smaller batches.\nRow array size (aka. batch size): \
        {size}\n Diagnostic record returned by SQLSetEnvAttr:\n{record}"
    )]
    InvalidRowArraySize {
        record: DiagnosticRecord,
        size: usize,
    },
}

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
        match self {
            SqlResult::Success(()) => SqlResult::Success(f()),
            SqlResult::SuccessWithInfo(()) => SqlResult::SuccessWithInfo(f()),
            SqlResult::Error { function } => SqlResult::Error { function },
        }
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

use std::io;

use thiserror::Error as ThisError;

use crate::handles::{log_diagnostics, AsHandle, Record as DiagnosticRecord, SqlResult};

#[derive(Debug, ThisError)]
/// Error type used to indicate a low level ODBC call returned with SQL_ERROR.
pub enum Error {
    /// Setting connection pooling match option failed. Exclusively emitted by
    /// [`crate::Environment::set_connection_pooling_matching`].
    #[error("Failed to set connection pooling matching.")]
    FailedSettingConnectionPoolingMatching,
    /// Setting connection pooling option failed. Exclusively emitted by
    /// [`crate::Environment::set_connection_pooling`].
    #[error("Failed to set connection pooling.")]
    FailedSettingConnectionPooling,
    /// Allocating the environment itself fails. Further diagnostics are not available, as they
    /// would be retrieved using the envirorment handle. Exclusively emitted by
    /// [`crate::Environment::new`].
    #[error("Failed to allocate ODBC Environment.")]
    FailedAllocatingEnvironment,
    /// This should never happen, given that ODBC driver manager and ODBC driver do not have any
    /// Bugs. Since we may link vs a bunch of these, better to be on the safe side.
    #[error(
        "No Diagnostics available. The ODBC function call to {} returned an error. Sadly neither the
        ODBC driver manager, nor the driver were polite enough to leave a diagnostic record
        specifying what exactly went wrong.", function
    )]
    NoDiagnostics {
        /// ODBC API call which returned error without producing a diagnostic record.
        function: &'static str,
    },
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

// Define that here rather than in `sql_result` mod to keep the `handles` modlue entirely agnostic
// about the top level `Error` type.
impl<T> SqlResult<T> {
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
                    Err(Error::NoDiagnostics { function })
                }
            }
        }
    }
}

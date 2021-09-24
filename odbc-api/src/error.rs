use std::io;

use thiserror::Error as ThisError;

use crate::handles::Record as DiagnosticRecord;

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
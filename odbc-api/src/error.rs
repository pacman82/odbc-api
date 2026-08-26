use std::{fmt, io, slice};

use thiserror::Error as ThisError;

use crate::handles::{Diagnostics, Record as DiagnosticRecord, SqlResult, log_diagnostics};

/// Non-empty collection of diagnostic records emitted by one ODBC call.
#[derive(Debug)]
pub struct DiagnosticRecords(Vec<DiagnosticRecord>);

impl DiagnosticRecords {
    /// Construct a non-empty diagnostic collection.
    pub fn new(first: DiagnosticRecord, additional_records: Vec<DiagnosticRecord>) -> Self {
        let mut records = Vec::with_capacity(additional_records.len() + 1);
        records.push(first);
        records.extend(additional_records);
        Self(records)
    }

    fn from_handle(handle: &impl Diagnostics) -> Option<Self> {
        let mut records = Vec::new();
        let mut record_number = 1;

        loop {
            let mut record = DiagnosticRecord::with_capacity(512);
            if !record.fill_from(handle, record_number) {
                break;
            }
            records.push(record);

            if record_number == i16::MAX {
                break;
            }
            record_number += 1;
        }

        (!records.is_empty()).then_some(Self(records))
    }

    /// Iterate over the diagnostic records in the order returned by ODBC.
    pub fn iter(&self) -> slice::Iter<'_, DiagnosticRecord> {
        self.0.iter()
    }

    /// Return the first diagnostic record.
    pub fn first(&self) -> &DiagnosticRecord {
        &self.0[0]
    }

    /// Return the last diagnostic record.
    pub fn last(&self) -> &DiagnosticRecord {
        &self.0[self.0.len() - 1]
    }

    pub(crate) fn into_last(mut self) -> DiagnosticRecord {
        self.0.pop().unwrap()
    }
}

impl fmt::Display for DiagnosticRecords {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, record) in self.iter().enumerate() {
            if index != 0 {
                f.write_str("\n")?;
            }
            record.fmt(f)?;
        }
        Ok(())
    }
}

/// Error indicating a failed allocation for a column buffer
#[derive(Debug)]
pub struct TooLargeBufferSize {
    /// Number of elements supposed to be in the buffer.
    pub num_elements: usize,
    /// Element size in the buffer in bytes.
    pub element_size: usize,
}

impl TooLargeBufferSize {
    /// Map the column allocation error to an [`crate::Error`] adding the context of which
    /// column caused the allocation error.
    pub fn add_context(self, buffer_index: u16) -> Error {
        Error::TooLargeColumnBufferSize {
            buffer_index,
            num_elements: self.num_elements,
            element_size: self.element_size,
        }
    }
}

#[cfg(feature = "odbc_version_3_5")]
const ODBC_VERSION_STRING: &str = "3.5";
#[cfg(not(feature = "odbc_version_3_5"))]
const ODBC_VERSION_STRING: &str = "3.80";

#[derive(Debug, ThisError)]
/// Error type used to indicate a low level ODBC call returned with SQL_ERROR.
pub enum Error {
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
        "No Diagnostics available. The ODBC function call to {} returned an error. Sadly neither \
        the ODBC driver manager, nor the driver were polite enough to leave a diagnostic record \
        specifying what exactly went wrong.",
        function
    )]
    NoDiagnostics {
        /// ODBC API call which returned error without producing a diagnostic record.
        function: &'static str,
    },
    /// SQL Error had been returned by a low level ODBC function call. Its diagnostic records are
    /// obtained and associated with this error.
    #[error("ODBC emitted an error calling '{function}':\n{records}")]
    Diagnostics {
        /// Diagnostic records returned by the ODBC driver manager and driver.
        records: DiagnosticRecords,
        /// ODBC API call which produced the diagnostic record
        function: &'static str,
    },
    /// A user dialog to complete the connection string has been aborted.
    #[error("The dialog shown to provide or complete the connection string has been aborted.")]
    AbortedConnectionStringCompletion,
    /// An error returned if we fail to set the ODBC version
    #[error(
        "The ODBC diver manager installed in your system does not seem to support ODBC API version \
        {ODBC_VERSION_STRING}. Which is required by this application. Most likely you need to \
        update your driver manager. Your driver manager is most likely unixODBC if you run on a \
        Linux. Diagnostic record returned by SQLSetEnvAttr:\n{0}"
    )]
    UnsupportedOdbcApiVersion(DiagnosticRecord),
    /// An error emitted by an `std::io::ReadBuf` implementation used as an input argument.
    #[error("Sending data to the database at statement execution time failed. IO error:\n{0}")]
    FailedReadingInput(io::Error),
    /// Driver returned "invalid attribute" then setting the row array size. Most likely the array
    /// size is too large. Instead of returing "option value changed (SQLSTATE 01S02)" as suggested
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
    #[error(
        "Tried to retrieve a value from the database. The value turned out to be `NULL` yet this \
        turned out to not be representable. So the application is written as if the value could \
        never be `NULL` in the datasource, yet the in actuallity a `NULL` has been returned. \
        Diagnostic record returned:\n{0}"
    )]
    UnableToRepresentNull(DiagnosticRecord),
    /// There are plenty of issues in the net about Oracle ODBC driver not supporting 64Bit. This
    /// message, should make it easier identify what is going on, since the message emmitted by,
    /// Oracles ODBC driver is a bit cryptic: `[Oracle][ODBC]Invalid SQL data type <-25>`.
    #[error(
        "SQLFetch came back with an error indicating you specified an invalid SQL Type. You very \
        likely did not do that however. Actually SQLFetch is not supposed to return that error \
        type.  You should have received it back than you were still binding columns or parameters. \
        All this is circumstancial evidence that you are using an Oracle Database and want to use \
        64Bit integers, which are not supported by Oracles ODBC driver manager. In case this \
        diagnose is wrong the original error is:\n{0}."
    )]
    OracleOdbcDriverDoesNotSupport64Bit(DiagnosticRecord),
    #[error(
        "There is not enough memory to allocate enough memory for a column buffer. Number of \
        elements requested for the column buffer: {num_elements}; Size needed to hold the largest \
        possible element: {element_size}."
    )]
    TooLargeColumnBufferSize {
        /// Zero based column buffer index. Note that this is different from the 1 based column
        /// index.
        buffer_index: u16,
        num_elements: usize,
        /// `usize::MAX` may be used to indicate a missing aupper bound of an element.
        element_size: usize,
    },
    #[error(
        "A value (at least one) is too large to be written into the allocated buffer without \
        truncation. Size in bytes indicated by ODBC driver: {indicator:?}"
    )]
    TooLargeValueForBuffer {
        /// Length of the complete value in bytes as reported by the ODBC driver. If the length is
        /// not known, this is `None`.
        indicator: Option<usize>,
        /// Index of the buffer in which the truncation occurred.
        buffer_index: usize,
    },
}

impl Error {
    /// Allows for mapping the error variant from the "catch all" diagnostic to a more specific one
    /// offering the oppertunity to provide context in the error message.
    fn provide_context_for_diagnostic<F>(self, f: F) -> Self
    where
        F: FnOnce(DiagnosticRecords, &'static str) -> Error,
    {
        if let Error::Diagnostics { records, function } = self {
            f(records, function)
        } else {
            self
        }
    }
}

/// Convinience for easily providing more context to errors without an additional call to `map_err`
pub(crate) trait ExtendResult {
    fn provide_context_for_diagnostic<F>(self, f: F) -> Self
    where
        F: FnOnce(DiagnosticRecords, &'static str) -> Error;
}

impl<T> ExtendResult for Result<T, Error> {
    fn provide_context_for_diagnostic<F>(self, f: F) -> Self
    where
        F: FnOnce(DiagnosticRecords, &'static str) -> Error,
    {
        self.map_err(|error| error.provide_context_for_diagnostic(f))
    }
}

impl SqlResult<()> {
    /// Use this instead of [`Self::into_result`] if you expect [`SqlResult::NoData`] to be a
    /// valid value. [`SqlResult::NoData`] is mapped to `Ok(false)`, all other success values are
    /// `Ok(true)`.
    pub fn into_result_bool(self, handle: &impl Diagnostics) -> Result<bool, Error> {
        self.on_success(|| true)
            .on_no_data(|| false)
            .into_result(handle)
    }
}

// Define that here rather than in `sql_result` mod to keep the `handles` module entirely agnostic
// about the top level `Error` type.
impl<T> SqlResult<T> {
    /// `true` for [`Self::SuccessWithInfo`] and [`Self::Error`]. If `true` one might expect
    /// diagnostic records to be present. If `false` it would indicate their absense.
    pub fn has_diganostics(&self) -> bool {
        matches!(
            self,
            SqlResult::SuccessWithInfo(_) | SqlResult::Error { function: _ }
        )
    }

    /// [`Self::Success`] and [`Self::SuccessWithInfo`] are mapped to Ok. In case of
    /// [`Self::SuccessWithInfo`] any diagnostics are logged. [`Self::Error`] is mapped to error.
    /// Other states [`Self::NoData]` and [`Self::NeedData`] would lead to a panic. Most ODBC
    /// functions are not suppossed to return these status codes.
    pub fn into_result(self, handle: &impl Diagnostics) -> Result<T, Error> {
        if self.has_diganostics() {
            log_diagnostics(handle);
        }
        self.into_result_without_logging(handle)
    }

    /// [`Self::Success`] and [`Self::SuccessWithInfo`] are mapped to Ok. [`Self::Error`] is mapped
    /// to error. Other states [`Self::NoData]` and [`Self::NeedData`] would lead to a panic. Most
    /// ODBC functions are not suppossed to return these status codes.
    ///
    /// In case of [`Self::Error`] or [`Self::SuccessWithInfo`] no logging of diagnostic records is
    /// performed by this method. You may want to use [Self::into_result`] instead.
    pub fn into_result_without_logging(self, handle: &impl Diagnostics) -> Result<T, Error> {
        match self {
            // The function has been executed successfully. Holds result.
            SqlResult::Success(value) | SqlResult::SuccessWithInfo(value) => Ok(value),
            SqlResult::Error { function } => {
                if let Some(records) = DiagnosticRecords::from_handle(handle) {
                    Err(Error::Diagnostics { records, function })
                } else {
                    // Anecdotal ways to reach this code paths:
                    //
                    // * Inserting a 64Bit integers into an Oracle Database.
                    // * Specifying invalid drivers (e.g. missing .so the driver itself depends on)
                    Err(Error::NoDiagnostics { function })
                }
            }
            SqlResult::NoData => {
                panic!(
                    "Unexepcted SQL_NO_DATA returned by ODBC function. Use `SqlResult::on_no_data` \
                    to handle it."
                )
            }
            SqlResult::NeedData => {
                panic!(
                    "Unexpected SQL_NEED_DATA returned by ODBC function. Use \
                    `SqlResult::on_need_data` to handle it."
                )
            }
            SqlResult::StillExecuting => panic!(
                "SqlResult must not be converted to result while the function is still executing."
            ),
        }
    }

    /// Maps [`SqlResult::Success`] and [`SqlResult::SuccessWithInfo`] to `Some`. Maps
    /// [`SqlResult::NoData`] to [`SqlResult::Success`] with `None`.
    pub fn or_no_data(self) -> SqlResult<Option<T>> {
        self.map(Some).on_no_data(|| None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handles::{DiagnosticResult, SqlChar, State};

    struct TwoDiagnostics;

    impl Diagnostics for TwoDiagnostics {
        fn diagnostic_record(
            &self,
            record_number: i16,
            _message_text: &mut [SqlChar],
        ) -> Option<DiagnosticResult> {
            let state = match record_number {
                1 => State(*b"HY000"),
                2 => State(*b"08001"),
                _ => return None,
            };
            Some(DiagnosticResult {
                state,
                native_error: 0,
                text_length: 0,
            })
        }
    }

    #[test]
    fn error_preserves_all_diagnostic_records() {
        let error = SqlResult::<()>::Error {
            function: "SQLDriverConnect",
        }
        .into_result_without_logging(&TwoDiagnostics)
        .unwrap_err();

        let Error::Diagnostics { records, .. } = error else {
            panic!("expected ODBC diagnostics");
        };
        let states = records
            .iter()
            .map(|record| record.state)
            .collect::<Vec<_>>();
        assert_eq!(states, [State(*b"HY000"), State(*b"08001")]);
    }
}

#[cfg(feature = "structured_logging")]
use super::slice_to_cow_utf8;
use super::{DiagnosticStream, Diagnostics};
use log::{Level, warn};

/// This function inspects all the diagnostics of an ODBC handle and logs their text messages. It
/// is going to print placeholder characters, if it cannot convert the message to UTF-8.
pub fn log_diagnostics(handle: &(impl Diagnostics + ?Sized)) {
    if log::max_level() < Level::Warn {
        // Early return to safe work creating all these log records in case we would not log
        // anything.
        return;
    }

    let mut diagnostic_stream = DiagnosticStream::new(handle);

    // Log results, while there are diagnostic records
    while let Some(record) = diagnostic_stream.next() {
        #[cfg(not(feature = "structured_logging"))]
        warn!("{record}");
        #[cfg(feature = "structured_logging")]
        warn!(
            target: "odbc",
            state = record.state.as_str(),
            native_error = record.native_error,
            message:% = slice_to_cow_utf8(&record.message);
            "Diagnostic"
        );
    }
}

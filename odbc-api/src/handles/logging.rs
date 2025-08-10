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
        warn!("{record}");
    }
}

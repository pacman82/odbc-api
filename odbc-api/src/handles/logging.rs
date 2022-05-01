use super::{diagnostics::Record, Diagnostics};
use log::{warn, Level};

/// This function inspects all the diagnostics of an ODBC handle and logs their text messages. It
/// is going to print placeholder characters, if it cannot convert the message to UTF-8.
pub fn log_diagnostics(handle: &(impl Diagnostics + ?Sized)) {
    if log::max_level() < Level::Warn {
        // Early return to safe work creating all these log records in case we would not log
        // anyhing.
        return;
    }

    let mut rec_number = 1;
    let mut rec = Record::default();

    // Log results, while there are diagnostic records
    while rec.fill_from(handle, rec_number) {
        warn!("{}", rec);
        // Prevent overflow. This is not that unlikely to happen, since some `execute` or `fetch`
        // calls can cause diagnostic messages for each row
        if rec_number == i16::MAX {
            warn!("Too many diagnostic records were generated. Not all could be logged.");
            break;
        }
        rec_number += 1;
    }
}

use crate::{as_handle::AsHandle, diagnostics::Record};
use log::warn;

/// This function inspectas all the diagnostics of an ODBC handle and logs their text messages. It
/// is going to print placeholder characters, if it cannot convert the message to UTF-8.
pub fn log_diagnostics(handle: &dyn AsHandle) {
    let mut rec_number = 1;
    let mut rec = Record::new();
    // Log results, while there are diagnostic records
    while rec.fill_from(handle, rec_number) {
        warn!("{}", rec);
        rec_number += 1;
    }
}

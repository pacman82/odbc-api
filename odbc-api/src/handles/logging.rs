use super::{Diagnostics, Record};
use log::{warn, Level};

/// This function inspects all the diagnostics of an ODBC handle and logs their text messages. It
/// is going to print placeholder characters, if it cannot convert the message to UTF-8.
pub fn log_diagnostics(handle: &(impl Diagnostics + ?Sized)) {
    if log::max_level() < Level::Warn {
        // Early return to safe work creating all these log records in case we would not log
        // anything.
        return;
    }

    let mut rec = Record::with_capacity(512);
    let mut rec_number = 1;

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

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, cmp::max};

    use crate::handles::{diagnostics::DiagnosticResult, SqlChar, State};

    use super::{log_diagnostics, Diagnostics};

    struct InfiniteDiagnostics {
        times_called: RefCell<usize>,
    }

    impl InfiniteDiagnostics {
        fn new() -> InfiniteDiagnostics {
            Self {
                times_called: RefCell::new(0),
            }
        }

        fn num_calls(&self) -> usize {
            *self.times_called.borrow()
        }
    }

    impl Diagnostics for InfiniteDiagnostics {
        fn diagnostic_record(
            &self,
            _rec_number: i16,
            _message_text: &mut [SqlChar],
        ) -> Option<DiagnosticResult> {
            *self.times_called.borrow_mut() += 1;
            Some(DiagnosticResult {
                state: State([0, 0, 0, 0, 0]),
                native_error: 0,
                text_length: 0,
            })
        }
    }

    /// This test is inspired by a bug caused from a fetch statement generating a lot of diagnostic
    /// messages.
    #[test]
    fn more_than_i16_max_diagnostic_records() {
        // Ensure log level is at least warn for test to work
        log::set_max_level(max(log::LevelFilter::Warn, log::max_level()));

        let spy = InfiniteDiagnostics::new();
        // We are quite happy if this terminates and does not overflow
        log_diagnostics(&spy);

        assert_eq!(spy.num_calls(), i16::MAX as usize)
    }
}

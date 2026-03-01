//! Test for logging is isolated in its own module to avoid messing with the global settings of
//! other, non-logging related tests.
use log::Level;
use odbc_api::handles::{DiagnosticResult, Diagnostics, SqlChar, State, log_diagnostics};

#[test]
fn emit_a_warning_for_each_diagnostic() {
    // Given a handle which would have two diagnostics
    struct DiagnosticStub;

    impl DiagnosticStub {
        fn write_message(text: &[u8], buf: &mut [SqlChar]) -> DiagnosticResult {
            let len = text.len().min(buf.len());
            for (i, &byte) in text[..len].iter().enumerate() {
                buf[i] = byte as SqlChar;
            }
            DiagnosticResult {
                state: State(*b"01000"),
                native_error: 0,
                text_length: text.len() as i16,
            }
        }
    }

    impl Diagnostics for DiagnosticStub {
        fn diagnostic_record(
            &self,
            rec_number: i16,
            message_text: &mut [SqlChar],
        ) -> Option<DiagnosticResult> {
            match rec_number {
                1 => Some(Self::write_message(b"first diagnostic", message_text)),
                2 => Some(Self::write_message(b"second diagnostic", message_text)),
                _ => None,
            }
        }
    }

    testing_logger::setup();

    // When logging the diagnostics of the handle
    log_diagnostics(&DiagnosticStub);

    // Both diagnostics are logged as warnings
    testing_logger::validate(|captured_logs| {
        assert_eq!(captured_logs.len(), 2);
        assert_eq!(captured_logs[0].level, Level::Warn);
        assert!(captured_logs[0].body.contains("first diagnostic"));
        assert_eq!(captured_logs[1].level, Level::Warn);
        assert!(captured_logs[1].body.contains("second diagnostic"));
    });
}

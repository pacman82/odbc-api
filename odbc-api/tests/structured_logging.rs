//! Test for structured logging is isolated in its own module to avoid messing with the global
//! settings of other, non-logging related tests.
#![cfg(feature = "structured_logging")]

use std::{cell::RefCell, collections::HashMap, sync::Once};

use log::{
    Level, LevelFilter, Log, Metadata, Record,
    kv::{self, Key, Value, VisitSource},
};
use odbc_api::handles::{DiagnosticResult, Diagnostics, SqlChar, State, log_diagnostics};

/// A log record captured by the test logger, including any structured key-value pairs.
struct CapturedLog {
    body: String,
    level: Level,
    target: String,
    kv: HashMap<String, String>,
}

thread_local! {
    static LOG_RECORDS: RefCell<Vec<CapturedLog>> = RefCell::new(Vec::new());
}

struct KvCollector(HashMap<String, String>);

impl<'kvs> VisitSource<'kvs> for KvCollector {
    fn visit_pair(&mut self, key: Key<'kvs>, value: Value<'kvs>) -> Result<(), kv::Error> {
        self.0.insert(key.to_string(), value.to_string());
        Ok(())
    }
}

struct KvLogger;

impl Log for KvLogger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        let mut collector = KvCollector(HashMap::new());
        let _ = record.key_values().visit(&mut collector);
        LOG_RECORDS.with(|records| {
            records.borrow_mut().push(CapturedLog {
                body: record.args().to_string(),
                level: record.level(),
                target: record.target().to_string(),
                kv: collector.0,
            });
        });
    }

    fn flush(&self) {}
}

static LOGGER: KvLogger = KvLogger;
static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        log::set_logger(&LOGGER).unwrap();
        log::set_max_level(LevelFilter::Trace);
    });
    LOG_RECORDS.with(|records| records.borrow_mut().clear());
}

fn validate<F>(asserter: F)
where
    F: FnOnce(&[CapturedLog]),
{
    LOG_RECORDS.with(|records| {
        asserter(&records.borrow());
        records.borrow_mut().clear();
    });
}

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

    setup();

    // When logging the diagnostics of the handle
    log_diagnostics(&DiagnosticStub);

    // Both diagnostics are captured as warnings with structured fields on the "odbc" target
    validate(|captured| {
        assert_eq!(captured.len(), 2);

        assert_eq!(captured[0].level, Level::Warn);
        assert_eq!(captured[0].target, "odbc");
        assert_eq!(captured[0].body, "Diagnostic");
        assert_eq!(
            captured[0].kv.get("state").map(String::as_str),
            Some("01000")
        );
        assert_eq!(
            captured[0].kv.get("native_error").map(String::as_str),
            Some("0")
        );
        assert_eq!(
            captured[0].kv.get("message").map(String::as_str),
            Some("first diagnostic")
        );

        assert_eq!(captured[1].level, Level::Warn);
        assert_eq!(captured[1].target, "odbc");
        assert_eq!(captured[1].body, "Diagnostic");
        assert_eq!(
            captured[1].kv.get("state").map(String::as_str),
            Some("01000")
        );
        assert_eq!(
            captured[1].kv.get("native_error").map(String::as_str),
            Some("0")
        );
        assert_eq!(
            captured[1].kv.get("message").map(String::as_str),
            Some("second diagnostic")
        );
    });
}

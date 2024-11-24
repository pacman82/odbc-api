use crate::handles::slice_to_cow_utf8;

use super::{
    as_handle::AsHandle,
    buffer::{clamp_small_int, mut_buf_ptr},
    SqlChar,
};
use odbc_sys::{SqlReturn, SQLSTATE_SIZE};
use std::fmt;

// Starting with odbc 5 we may be able to specify utf8 encoding. Until then, we may need to fall
// back on the 'W' wide function calls.
#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
use odbc_sys::SQLGetDiagRecW as sql_get_diag_rec;

#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
use odbc_sys::SQLGetDiagRec as sql_get_diag_rec;

/// A buffer large enough to hold an `SOLState` for diagnostics
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct State(pub [u8; SQLSTATE_SIZE]);

impl State {
    /// Can be returned from SQLDisconnect
    pub const INVALID_STATE_TRANSACTION: State = State(*b"25000");
    /// Given the specified Attribute value, an invalid value was specified in ValuePtr.
    pub const INVALID_ATTRIBUTE_VALUE: State = State(*b"HY024");
    /// An invalid data type has been bound to a statement. Is also returned by SQLFetch if trying
    /// to fetch into a 64Bit Integer Buffer.
    pub const INVALID_SQL_DATA_TYPE: State = State(*b"HY004");
    /// String or binary data returned for a column resulted in the truncation of nonblank character
    /// or non-NULL binary data. If it was a string value, it was right-truncated.
    pub const STRING_DATA_RIGHT_TRUNCATION: State = State(*b"01004");
    /// StrLen_or_IndPtr was a null pointer and NULL data was retrieved.
    pub const INDICATOR_VARIABLE_REQUIRED_BUT_NOT_SUPPLIED: State = State(*b"22002");

    /// Drops terminating zero and changes char type, if required
    pub fn from_chars_with_nul(code: &[SqlChar; SQLSTATE_SIZE + 1]) -> Self {
        // `SQLGetDiagRecW` returns ODBC state as wide characters. This constructor converts the
        //  wide characters to narrow and drops the terminating zero.

        let mut ascii = [0; SQLSTATE_SIZE];
        for (index, letter) in code[..SQLSTATE_SIZE].iter().copied().enumerate() {
            ascii[index] = letter as u8;
        }
        State(ascii)
    }

    /// View status code as string slice for displaying. Must always succeed as ODBC status code
    /// always consist of ASCII characters.
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap()
    }
}

/// Result of [`Diagnostic::diagnostic_record`].
#[derive(Debug, Clone, Copy)]
pub struct DiagnosticResult {
    /// A five-character SQLSTATE code (and terminating NULL) for the diagnostic record
    /// `rec_number`. The first two characters indicate the class; the next three indicate the
    /// subclass. For more information, see [SQLSTATE][1]s.
    /// [1]: https://docs.microsoft.com/sql/odbc/reference/develop-app/sqlstates
    pub state: State,
    /// Native error code specific to the data source.
    pub native_error: i32,
    /// The length of the diagnostic message reported by ODBC (excluding the terminating zero).
    pub text_length: i16,
}

/// Report diagnostics from the last call to an ODBC function using a handle.
pub trait Diagnostics {
    /// Call this method to retrieve diagnostic information for the last call to an ODBC function.
    ///
    /// Returns the current values of multiple fields of a diagnostic record that contains error,
    /// warning, and status information
    ///
    /// See: [Diagnostic Messages][1]
    ///
    /// # Arguments
    ///
    /// * `rec_number` - Indicates the status record from which the application seeks information.
    ///   Status records are numbered from 1. Function panics for values smaller < 1.
    /// * `message_text` - Buffer in which to return the diagnostic message text string. If the
    ///   number of characters to return is greater than the buffer length, the message is
    ///   truncated. To determine that a truncation occurred, the application must compare the
    ///   buffer length to the actual number of bytes available, which is found in
    ///   [`self::DiagnosticResult::text_length]`
    ///
    /// # Result
    ///
    /// * `Some(rec)` - The function successfully returned diagnostic information.
    ///   message. No diagnostic records were generated.
    /// * `None` - `rec_number` was greater than the number of diagnostic records that existed for
    ///   the specified Handle. The function also returns `NoData` for any positive `rec_number` if
    ///   there are no diagnostic records available.
    ///
    /// [1]: https://docs.microsoft.com/sql/odbc/reference/develop-app/diagnostic-messages
    fn diagnostic_record(
        &self,
        rec_number: i16,
        message_text: &mut [SqlChar],
    ) -> Option<DiagnosticResult>;

    /// Call this method to retrieve diagnostic information for the last call to an ODBC function.
    /// This method builds on top of [`Self::diagnostic_record`], if the message does not fit in the
    /// buffer, it will grow the message buffer and extract it again.
    ///
    /// See: [Diagnostic Messages][1]
    ///
    /// # Arguments
    ///
    /// * `rec_number` - Indicates the status record from which the application seeks information.
    ///   Status records are numbered from 1. Function panics for values smaller < 1.
    /// * `message_text` - Buffer in which to return the diagnostic message text string. If the
    ///   number of characters to return is greater than the buffer length, the buffer will be grown
    ///   to be large enough to hold it.
    ///
    /// # Result
    ///
    /// * `Some(rec)` - The function successfully returned diagnostic information.
    ///   message. No diagnostic records were generated. To determine that a truncation occurred,
    ///   the application must compare the buffer length to the actual number of bytes available,
    ///   which is found in [`self::DiagnosticResult::text_length]`.
    /// * `None` - `rec_number` was greater than the number of diagnostic records that existed for
    ///   the specified Handle. The function also returns `NoData` for any positive `rec_number` if
    ///   there are no diagnostic records available.
    ///
    /// [1]: https://docs.microsoft.com/sql/odbc/reference/develop-app/diagnostic-messages
    fn diagnostic_record_vec(
        &self,
        rec_number: i16,
        message_text: &mut Vec<SqlChar>,
    ) -> Option<DiagnosticResult> {
        // Use all the memory available in the buffer, but don't allocate any extra.
        let cap = message_text.capacity();
        message_text.resize(cap, 0);

        self.diagnostic_record(rec_number, message_text)
            .map(|mut result| {
                let mut text_length = result.text_length.try_into().unwrap();

                // Check if the buffer has been large enough to hold the message.
                if text_length > message_text.len() {
                    // The `message_text` buffer was too small to hold the requested diagnostic message.
                    // No diagnostic records were generated. To determine that a truncation occurred,
                    // the application must compare the buffer length to the actual number of bytes
                    // available, which is found in `DiagnosticResult::text_length`.

                    // Resize with +1 to account for terminating zero
                    message_text.resize(text_length + 1, 0);

                    // Call diagnostics again with the larger buffer. Should be a success this time if
                    // driver isn't buggy.
                    result = self.diagnostic_record(rec_number, message_text).unwrap();
                }
                // Now `message_text` has been large enough to hold the entire message.

                // Some drivers pad the message with null-chars (which is still a valid C string,
                // but not a valid Rust string).
                while text_length > 0 && message_text[text_length - 1] == 0 {
                    text_length -= 1;
                }
                // Resize Vec to hold exactly the message.
                message_text.resize(text_length, 0);

                result
            })
    }
}

impl<T: AsHandle + ?Sized> Diagnostics for T {
    fn diagnostic_record(
        &self,
        rec_number: i16,
        message_text: &mut [SqlChar],
    ) -> Option<DiagnosticResult> {
        // Diagnostic records in ODBC are indexed starting with 1
        assert!(rec_number > 0);

        // The total number of characters (excluding the terminating NULL) available to return in
        // `message_text`.
        let mut text_length = 0;
        let mut state = [0; SQLSTATE_SIZE + 1];
        let mut native_error = 0;
        let ret = unsafe {
            sql_get_diag_rec(
                self.handle_type(),
                self.as_handle(),
                rec_number,
                state.as_mut_ptr(),
                &mut native_error,
                mut_buf_ptr(message_text),
                clamp_small_int(message_text.len()),
                &mut text_length,
            )
        };

        let result = DiagnosticResult {
            state: State::from_chars_with_nul(&state),
            native_error,
            text_length,
        };

        match ret {
            SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => Some(result),
            SqlReturn::NO_DATA => None,
            SqlReturn::ERROR => panic!("rec_number argument of diagnostics must be > 0."),
            unexpected => panic!("SQLGetDiagRec returned: {unexpected:?}"),
        }
    }
}

/// ODBC Diagnostic Record
///
/// The `description` method of the `std::error::Error` trait only returns the message. Use
/// `std::fmt::Display` to retrieve status code and other information.
#[derive(Default)]
pub struct Record {
    /// All elements but the last one, may not be null. The last one must be null.
    pub state: State,
    /// Error code returned by Driver manager or driver
    pub native_error: i32,
    /// Buffer containing the error message. The buffer already has the correct size, and there is
    /// no terminating zero at the end.
    pub message: Vec<SqlChar>,
}

impl Record {
    /// Creates an empty diagnostic record with at least the specified capacity for the message.
    /// Using a buffer with a size different from zero then filling the diagnostic record may safe a
    /// second function call to `SQLGetDiagRec`.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            message: Vec::with_capacity(capacity),
            ..Default::default()
        }
    }

    /// Fill this diagnostic `Record` from any ODBC handle.
    ///
    /// # Return
    ///
    /// `true` if a record has been found, `false` if not.
    pub fn fill_from(&mut self, handle: &(impl Diagnostics + ?Sized), record_number: i16) -> bool {
        match handle.diagnostic_record_vec(record_number, &mut self.message) {
            Some(result) => {
                self.state = result.state;
                self.native_error = result.native_error;
                true
            }
            None => false,
        }
    }
}

impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = slice_to_cow_utf8(&self.message);

        write!(
            f,
            "State: {}, Native error: {}, Message: {}",
            self.state.as_str(),
            self.native_error,
            message,
        )
    }
}

impl fmt::Debug for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {

    use crate::handles::diagnostics::State;

    use super::Record;

    #[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
    fn to_vec_sql_char(text: &str) -> Vec<u16> {
        text.encode_utf16().collect()
    }

    #[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
    fn to_vec_sql_char(text: &str) -> Vec<u8> {
        text.bytes().collect()
    }

    #[test]
    fn formatting() {
        // build diagnostic record
        let message = to_vec_sql_char("[Microsoft][ODBC Driver Manager] Function sequence error");
        let rec = Record {
            state: State(*b"HY010"),
            message,
            ..Record::default()
        };

        // test formatting
        assert_eq!(
            format!("{rec}"),
            "State: HY010, Native error: 0, Message: [Microsoft][ODBC Driver Manager] \
             Function sequence error"
        );
    }
}

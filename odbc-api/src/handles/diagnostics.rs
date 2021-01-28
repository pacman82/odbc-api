use super::{
    as_handle::AsHandle,
    buffer::{clamp_small_int, mut_buf_ptr},
};
use odbc_sys::{SQLGetDiagRecW, SqlReturn, SQLSTATE_SIZE};
use std::{convert::TryInto, fmt};
use widestring::{U16CStr, U16Str};

/// A buffer large enough to hold an `SOLState` for diagnostics and a terminating zero.
pub type State = [u16; SQLSTATE_SIZE + 1];

/// Result of `diagnostics`.
#[derive(Debug, Clone, Copy)]
pub struct DiagResult {
    /// A five-character SQLSTATE code (and terminating NULL) for the diagnostic record
    /// `rec_number`. The first two characters indicate the class; the next three indicate the
    /// subclass. For more information, see [SQLSTATE][1]s.
    /// [1]: https://docs.microsoft.com/sql/odbc/reference/develop-app/sqlstates
    pub state: State,
    /// Native error code specific to the data source.
    pub native_error: i32,
}

/// Call this function to retrieve diagnostic information for the last method call.
///
/// Returns the current values of multiple fields of a diagnostic record that contains error,
/// warning, and status information.
///
/// # Arguments
///
/// * `handle` - Handle used to query for diagnostic records.
/// * `rec_number` - Indicates the status record from which the application seeks information.
/// Status records are numbered from 1. Function panics for values smaller < 1.
/// * `message_text` - Buffer in which to return the diagnostic message text string. If the
/// number of characters to return is greater than the buffer length, the buffer will be grown to be
/// large enough to hold it.
/// [Diagnostic Messages][1]
///
/// # Result
///
/// * `Some(rec)` - The function successfully returned diagnostic information.
/// message. No diagnostic records were generated. To determine that a truncation occurred, the
/// application must compare the buffer length to the actual number of bytes available, which is
/// found in `DiagResult::text_length`.
/// * `None` - `rec_number` was greater than the number of diagnostic records that existed for the
/// specified Handle. The function also returns `NoData` for any positive `rec_number` if there are
/// no diagnostic records available.
///
/// [1]: https://docs.microsoft.com/sql/odbc/reference/develop-app/diagnostic-messages
pub fn diagnostics(
    handle: &dyn AsHandle,
    rec_number: i16,
    message_text: &mut Vec<u16>,
) -> Option<DiagResult> {
    assert!(rec_number > 0);

    // Use all the memory available in the buffer, but don't allocate any extra.
    let cap = message_text.capacity();
    message_text.resize(cap, 0);

    // The total number of characters (excluding the terminating NULL) available to return in
    // `message_text`.
    let mut text_length = 0;
    let mut state = [0; SQLSTATE_SIZE + 1];
    let mut native_error = 0;
    let ret = unsafe {
        // Starting with odbc 5 we may be able to specify utf8 encoding. until then, we may need to
        // fall back on the 'W' wide function calls.
        SQLGetDiagRecW(
            handle.handle_type(),
            handle.as_handle(),
            rec_number,
            state.as_mut_ptr(),
            &mut native_error,
            mut_buf_ptr(message_text),
            clamp_small_int(message_text.len()),
            &mut text_length,
        )
    };
    let result = DiagResult {
        state,
        native_error,
    };
    let mut text_length: usize = text_length.try_into().unwrap();
    match ret {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => {
            // Check if the buffer has been large enough to hold the message.
            if text_length > message_text.len() {
                // The `message_text` buffer was too small to hold the requested diagnostic message.
                // No diagnostic records were generated. To determine that a truncation occurred,
                // the application must compare the buffer length to the actual number of bytes
                // available, which is found in `DiagResult::text_length`.

                // Resize with +1 to account for terminating zero
                message_text.resize(text_length + 1, 0);

                // Call diagnostics again with the larger buffer. Should be a success this time if
                // driver isn't buggy.
                diagnostics(handle, rec_number, message_text)
            } else {
                // `message_text` has been large enough to hold the entire message.

                // Some drivers pad the message with null-chars (which is still a valid C string,
                // but not a valid Rust string).
                while text_length > 0 && message_text[text_length - 1] == 0 {
                    text_length -= 1;
                }
                // Resize Vec to hold exactly the message.
                message_text.resize(text_length, 0);

                Some(result)
            }
        }
        SqlReturn::NO_DATA => None,
        SqlReturn::ERROR => panic!("rec_number argument of diagnostics must be > 0."),
        unexpected => panic!("SQLGetDiagRec returned: {:?}", unexpected),
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
    pub message: Vec<u16>,
}

impl Record {
    /// Fill this diagnostic `Record` from any ODBC handle.
    ///
    /// # Return
    ///
    /// `true` if a record has been found, `false` if not.
    pub fn fill_from(&mut self, handle: &dyn AsHandle, record_number: i16) -> bool {
        match diagnostics(handle, record_number, &mut self.message) {
            Some(result) => {
                self.state = result.state;
                self.native_error = result.native_error;
                true
            }
            None => false,
        }
    }

    /// True if state is 25000 invalid state transaction.
    pub fn is_invalid_state_transaction(&self) -> bool {
        self.state == [50, 53, 48, 48, 48, 0]
    }
}

impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let state = U16CStr::from_slice_with_nul(&self.state);

        let message = U16Str::from_slice(&self.message);

        write!(
            f,
            "State: {}, Native error: {}, Message: {}",
            state
                .map(U16CStr::to_string_lossy)
                .unwrap_or_else(|e| format!("Error decoding state: {}", e)),
            self.native_error,
            message.to_string_lossy(),
        )
    }
}

impl fmt::Debug for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod test {

    use super::Record;

    #[test]
    fn formatting() {
        // build diagnostic record
        let message: Vec<_> = "[Microsoft][ODBC Driver Manager] Function sequence error"
            .encode_utf16()
            .collect();
        let mut rec = Record::default();

        for (index, letter) in "HY010".encode_utf16().enumerate() {
            rec.state[index] = letter;
        }
        rec.message = message;

        // test formatting
        assert_eq!(
            format!("{}", rec),
            "State: HY010, Native error: 0, Message: [Microsoft][ODBC Driver Manager] \
             Function sequence error"
        );
    }
}

//! Provides basic abstraction over valid (i.e. allocated ODBC handles).
//!
//! Two decisions are already baked into this module:
//!
//! * Treat warnings by logging them with `log`.
//! * Use the Unicode (wide) variants of the ODBC API.

mod any_handle;
mod bind;
mod buffer;
mod column_description;
mod connection;
mod data_type;
mod descriptor;
mod diagnostics;
mod environment;
mod logging;
mod sql_char;
mod sql_result;
mod statement;
mod statement_connection;

pub use self::{
    any_handle::AnyHandle,
    bind::{CData, CDataMut, DelayedInput, HasDataType},
    column_description::{ColumnDescription, Nullability},
    connection::Connection,
    data_type::DataType,
    descriptor::Descriptor,
    diagnostics::{DiagnosticStream, Diagnostics, Record, State},
    environment::Environment,
    logging::log_diagnostics,
    sql_char::{OutputStringBuffer, SqlChar, SqlText, SzBuffer, slice_to_cow_utf8, slice_to_utf8},
    sql_result::SqlResult,
    statement::{AsStatementRef, ParameterDescription, Statement, StatementImpl, StatementRef},
    statement_connection::{StatementParent, StatementConnection},
};

pub(crate) use self::data_type::ASSUMED_MAX_LENGTH_OF_W_VARCHAR;

use log::debug;
use odbc_sys::{Handle, HandleType, SQLFreeHandle, SqlReturn};
use std::thread::panicking;

/// Helper function freeing a handle and panicking on errors. Yet if the drop is triggered during
/// another panic, the function will simply ignore errors from failed drops.
///
/// # Safety
///
/// `handle` Must be a valid ODBC handle and `handle_type` must match its type.
pub unsafe fn drop_handle(handle: Handle, handle_type: HandleType) {
    match unsafe { SQLFreeHandle(handle_type, handle) } {
        SqlReturn::SUCCESS => {
            debug!("SQLFreeHandle dropped {handle:?} of type {handle_type:?}.");
        }
        other => {
            // Avoid panicking, if we already have a panic. We don't want to mask the
            // original error.
            if !panicking() {
                panic!("SQLFreeHandle failed with error code: {:?}", other.0)
            }
        }
    }
}

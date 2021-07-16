//! Provides basic abstraction over valid (i.e. allocated ODBC handles).
//!
//! Two decisions are already baked into this module:
//!
//! * Treat warnings by logging them with `log`.
//! * Use the Unicode (wide) variants of the ODBC API.

mod as_handle;
mod bind;
mod buffer;
mod column_description;
mod connection;
mod data_type;
mod diagnostics;
mod environment;
mod error;
mod logging;
mod statement;

pub use {
    as_handle::AsHandle,
    bind::{CData, CDataMut, DelayedInput, HasDataType},
    buffer::OutputStringBuffer,
    column_description::{ColumnDescription, Nullability},
    connection::Connection,
    data_type::DataType,
    diagnostics::{Record, State},
    environment::Environment,
    error::Error,
    statement::{ParameterDescription, Statement, StatementImpl},
};

use odbc_sys::{Handle, HandleType, SQLFreeHandle, SqlReturn};
use std::thread::panicking;

/// Helper function freeing a handle and panicking on errors. Yet if the drop is triggered during
/// another panic, the function will simply ignore errors from failed drops.
unsafe fn drop_handle(handle: Handle, handle_type: HandleType) {
    match SQLFreeHandle(handle_type, handle) {
        SqlReturn::SUCCESS => (),
        other => {
            // Avoid panicking, if we already have a panic. We don't want to mask the
            // original error.
            if !panicking() {
                panic!("Unexpected return value of SQLFreeHandle: {:?}", other)
            }
        }
    }
}

//! Provides basic abstraction over valid (i.e. allocated ODBC handles). Two decisions are already
//! baked into this module:
//! * Treat warnings by logging them with `log`.
//! * Use the Unicode (wide) variants of the ODBC API.

mod as_handle;
mod buffer;
mod diagnostics;
mod error;
mod logging;
mod connection;
mod environment;
mod statement;

pub use {
    as_handle::AsHandle, connection::Connection, environment::Environment, statement::Statement, error::Error
};

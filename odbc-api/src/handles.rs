//! Provides basic abstraction over valid (i.e. allocated ODBC handles). Two decisions are already
//! baked into this module:
//! * Treat warnings by logging them with `log`.
//! * Use the Unicode (wide) variants of the ODBC API.

mod as_handle;
mod buffer;
mod column_description;
mod connection;
mod data_type;
mod description;
mod diagnostics;
mod environment;
mod error;
mod logging;
mod statement;

pub use {
    as_handle::AsHandle,
    column_description::{ColumnDescription, Nullable},
    connection::Connection,
    data_type::DataType,
    description::Description,
    diagnostics::Record,
    environment::Environment,
    error::Error,
    statement::Statement,
};

//! # ODBC
//!
//! This library aims in helping you write applications which utilize ODBC (Open Database
//! Connectivity) standard to query databases. See the [guide] for more information and code
//! examples.
//!
//! [guide]: ../odbc_api/guide/index.html

mod connection;
mod cursor;
mod environment;
mod parameters;
mod prepared;

pub mod buffers;
pub mod handles;
pub mod guide;

pub use self::{
    connection::Connection,
    cursor::{Cursor, CursorImpl, RowSetBuffer, RowSetCursor},
    environment::Environment,
    handles::{ColumnDescription, DataType, Error, Nullable},
    parameters::{Parameters, VarCharParam, WithDataType},
    prepared::Prepared,
};
// Reexports
/// Reexports `odbc-sys` as sys to enable applications to always use the same version as this crate.
pub use odbc_sys as sys;
pub use widestring::{U16Str, U16String};

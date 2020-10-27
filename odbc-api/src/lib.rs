//! # ODBC
//!
//! ODBC (Open Database Connectivity) is a Database standard. This library aims in helping you write
//! Applications which utilize ODBC to query databases.

mod parameters;
mod connection;
mod cursor;
mod environment;

pub mod buffers;
pub mod handles;

pub use self::{
    parameters::Parameters,
    connection::Connection,
    cursor::{Cursor, RowSetBuffer, RowSetCursor},
    environment::Environment,
    handles::{ColumnDescription, DataType, Error, Nullable},
};
// Reexports
/// Reexports `odbc-sys` as sys to enable applications to always use the same version as this crate.
pub use odbc_sys as sys;
pub use widestring::{U16Str, U16String};

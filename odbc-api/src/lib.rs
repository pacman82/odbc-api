//! # ODBC
//!
//! ODBC (Open Database Connectivity) is a Database standard. This library aims in helping you write
//! Applicatinos which utilize ODBC to query databases.

mod connection;
mod cursor;
mod environment;

pub mod handles;
pub mod buffers;

pub use self::{
    connection::Connection,
    cursor::{Cursor, RowSetBuffer, RowSetCursor},
    environment::Environment,
    handles::{ColumnDescription, Error, Nullable},
};
// Rexports
/// Rexport `odbc-sys` as sys to enable applications to always use the same version as this crate.
pub use odbc_sys as sys;
pub use widestring::{U16Str, U16String};

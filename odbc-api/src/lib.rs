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
mod into_parameter;
mod parameter_collection;
mod prepared;
mod fixed_sized;

pub mod buffers;
pub mod guide;
pub mod handles;
pub mod parameter;

pub use self::{
    connection::Connection,
    cursor::{Cursor, CursorImpl, RowSetBuffer, RowSetCursor},
    environment::Environment,
    fixed_sized::Bit,
    handles::{ColumnDescription, DataType, Error, Nullable},
    into_parameter::IntoParameter,
    parameter::Parameter,
    parameter_collection::ParameterCollection,
    prepared::Prepared,
};
// Reexports
/// Reexports `odbc-sys` as sys to enable applications to always use the same version as this
/// crate.
pub use odbc_sys as sys;
pub use widestring::{U16Str, U16String};

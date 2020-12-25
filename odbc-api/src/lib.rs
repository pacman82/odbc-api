//! # About
//!
//! `odbc-api` enables you to write applications which utilize ODBC (Open Database Connectivity)
//! standard to access databases. See the [`guide`] for more information and code
//! examples.

mod connection;
mod cursor;
mod environment;
mod fixed_sized;
mod into_parameter;
mod nullable;
mod parameter_collection;
mod prepared;

pub mod buffers;
pub mod guide;
pub mod handles;
pub mod parameter;

pub use self::{
    connection::Connection,
    cursor::{Cursor, CursorImpl, RowSetBuffer, RowSetCursor},
    environment::{DataSourceInfo, DriverInfo, Environment},
    fixed_sized::Bit,
    handles::{ColumnDescription, DataType, Error, Nullability},
    into_parameter::IntoParameter,
    nullable::Nullable,
    parameter::InputParameter,
    parameter_collection::ParameterCollection,
    prepared::Prepared,
};
// Reexports
/// Reexports `odbc-sys` as sys to enable applications to always use the same version as this
/// crate.
pub use odbc_sys as sys;
pub use widestring::{U16Str, U16String};

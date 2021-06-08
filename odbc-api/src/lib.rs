//! # About
//!
//! `odbc-api` enables you to write applications which utilize ODBC (Open Database Connectivity)
//! standard to access databases. See the [`guide`] for more information and code
//! examples.

mod connection;
mod cursor;
mod environment;
mod execute;
mod fixed_sized;
mod into_parameter;
mod nullable;
mod parameter_collection;
mod preallocated;
mod prepared;

pub mod buffers;
pub mod guide;
pub mod handles;
pub mod parameter;

pub use self::{
    connection::{escape_attribute_value, Connection},
    cursor::{Cursor, CursorImpl, CursorRow, RowSetBuffer, RowSetCursor},
    environment::{DataSourceInfo, DriverCompleteOption, DriverInfo, Environment},
    fixed_sized::Bit,
    handles::{ColumnDescription, DataType, Error, ExtendedColumnDescription, Nullability},
    into_parameter::IntoParameter,
    nullable::Nullable,
    parameter::{InputParameter, Out, Output, Parameter},
    parameter_collection::ParameterCollection,
    preallocated::Preallocated,
    prepared::Prepared,
};
// Reexports
/// Reexports `odbc-sys` as sys to enable applications to always use the same version as this
/// crate.
pub use odbc_sys as sys;
pub use widestring::{U16Str, U16String};

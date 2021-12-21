//! # About
//!
//! `odbc-api` enables you to write applications which utilize ODBC (Open Database Connectivity)
//! standard to access databases. See the [`guide`] for more information and code
//! examples.

mod borrow_mut_statement;
mod connection;
mod cursor;
mod driver_complete_option;
mod environment;
mod error;
mod execute;
mod fixed_sized;
mod into_parameter;
mod nullable;
mod parameter_collection;
mod preallocated;
mod prebound;
mod prepared;
mod result_set_metadata;
mod statement_connection;

pub mod buffers;
pub mod guide;
pub mod handles;
pub mod parameter;

pub use self::{
    connection::{escape_attribute_value, Connection},
    cursor::{Cursor, CursorImpl, CursorRow, RowSetBuffer, RowSetCursor},
    driver_complete_option::DriverCompleteOption,
    environment::{DataSourceInfo, DriverInfo, Environment},
    error::Error,
    fixed_sized::Bit,
    handles::{ColumnDescription, DataType, Nullability},
    into_parameter::IntoParameter,
    nullable::Nullable,
    parameter::{InputParameter, Out, Output, ParameterRef},
    parameter_collection::ParameterCollection,
    preallocated::Preallocated,
    prebound::Prebound,
    prepared::Prepared,
    result_set_metadata::ResultSetMetadata,
    statement_connection::StatementConnection,
};
// Reexports
pub use force_send_sync;
/// Reexports `odbc-sys` as sys to enable applications to always use the same version as this
/// crate.
pub use odbc_sys as sys;
pub use widestring::{U16Str, U16String};

//! This module contains buffers intended to be bound to ODBC statement handles.
mod fixed_sized;
mod text_column;
mod text_row_set;

use odbc_sys::{CDataType, Len, Pointer};

pub use self::{
    fixed_sized::{
        Bit, FixedSizedCType, OptBitColumn, OptDateColumn, OptF32Column, OptF64Column,
        OptFixedSizedColumn, OptI32Column, OptI64Column, OptI8Column, OptNumericColumn,
        OptTimeColumn, OptTimestampColumn, OptU8Column,
    },
    text_column::TextColumn,
    text_row_set::TextRowSet,
};

/// Arguments used to describe the buffer then binding this column to a cursor.
pub struct BindColArgs {
    /// The identifier of the C data type of the `value` buffer. When it is retrieving data from
    /// the data source with `fetch`, the driver converts the data to this type. When it sends
    /// data to the source, the driver converts the data from this type.
    pub target_type: CDataType,
    /// Pointer to the data buffer to bind to the column.Start address of the value buffer.
    pub target_value: Pointer,
    /// Length of a target value in bytes. For a single element in case of block fetching data.
    pub target_length: Len,
    /// Buffer is going to hold length or indicator values. Can be null if the column is not
    /// nullable and elements are of fixed length.
    pub indicator: *mut Len,
}

/// A type implementing this trait can be bound to a column of a cursor.
pub unsafe trait ColumnBuffer {
    /// Arguments used to describe the buffer then binding this column to a cursor.
    fn bind_arguments(&mut self) -> BindColArgs;
}

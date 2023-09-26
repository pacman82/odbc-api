//! This module contains buffers intended to be bound to ODBC statement handles.

mod any_buffer;
mod bin_column;
mod column_with_indicator;
mod columnar;
mod description;
mod indicator;
mod item;
mod text_column;

pub use self::{
    any_buffer::{AnyBuffer, AnySlice, AnySliceMut, ColumnarAnyBuffer},
    bin_column::{BinColumn, BinColumnIt, BinColumnSliceMut, BinColumnView},
    column_with_indicator::{NullableSlice, NullableSliceMut},
    columnar::{ColumnBuffer, ColumnarBuffer, TextRowSet},
    description::BufferDesc,
    indicator::{Indicator, TruncationDiagnostics},
    item::Item,
    text_column::{
        CharColumn, TextColumn, TextColumnIt, TextColumnSliceMut, TextColumnView, WCharColumn,
    },
};

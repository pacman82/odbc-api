//! This module contains buffers intended to be bound to ODBC statement handles.

mod any_buffer;
mod bin_column;
mod column_with_indicator;
mod columnar;
mod description;
mod indicator;
mod item;
mod text_column;

#[allow(deprecated)]
pub use self::{
    any_buffer::{
        AnyBuffer, AnyColumnBuffer, AnyColumnSliceMut, AnyColumnView, AnySlice, AnySliceMut,
        ColumnarAnyBuffer,
    },
    bin_column::{BinColumn, BinColumnIt, BinColumnSliceMut, BinColumnView},
    column_with_indicator::{NullableSlice, NullableSliceMut},
    columnar::{ColumnBuffer, ColumnProjections, ColumnarBuffer, TextRowSet},
    description::{BufferDescription, BufferKind},
    indicator::Indicator,
    item::Item,
    text_column::{
        CharColumn, TextColumn, TextColumnIt, TextColumnSliceMut, TextColumnView, WCharColumn,
    },
};

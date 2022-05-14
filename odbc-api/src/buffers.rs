//! This module contains buffers intended to be bound to ODBC statement handles.

mod any_column_buffer;
mod bin_column;
mod column_with_indicator;
mod columnar;
mod description;
mod indicator;
mod item;
mod text_column;

use crate::Error;

pub use self::{
    any_column_buffer::{AnyColumnBuffer, AnyColumnView, AnyColumnViewMut, ColumnarAnyBuffer},
    bin_column::{BinColumn, BinColumnIt, BinColumnView, BinColumnWriter},
    column_with_indicator::{NullableSlice, NullableSliceMut},
    columnar::{ColumnBuffer, ColumnProjections, ColumnarBuffer, TextRowSet},
    description::{BufferDescription, BufferKind},
    indicator::Indicator,
    item::Item,
    text_column::{
        CharColumn, TextColumn, TextColumnIt, TextColumnView, TextColumnWriter, WCharColumn,
    },
};

/// Convinience function allocating a [`ColumnarBuffer`] fitting the buffer descriptions. If not
/// enough memory is available to allocate the buffers this function fails with
/// [`Error::TooLargeColumnBufferSize`]. This function is slower than
/// [`self::buffer_from_description`] which would just panic if not enough memory is available for
/// allocation.
#[deprecated(
    since = "0.40.2",
    note = "Please use `ColumnarAnyBuffer::try_from_description` instead"
)]
pub fn try_buffer_from_description(
    capacity: usize,
    descs: impl Iterator<Item = BufferDescription>,
) -> Result<ColumnarBuffer<AnyColumnBuffer>, Error> {
    ColumnarAnyBuffer::try_from_description(capacity, descs)
}

/// Convinience function allocating a [`ColumnarBuffer`] fitting the buffer descriptions.
#[deprecated(
    since = "0.40.2",
    note = "Please use `ColumnarAnyBuffer::from_description` instead"
)]
pub fn buffer_from_description(
    capacity: usize,
    descs: impl Iterator<Item = BufferDescription>,
) -> ColumnarBuffer<AnyColumnBuffer> {
    ColumnarAnyBuffer::from_description(capacity, descs)
}

/// Allows you to pass the buffer descriptions together with a one based column index referring the
/// column, the buffer is supposed to bind to. This allows you also to ignore columns in a result
/// set, by not binding them at all. There is no restriction on the order of column indices passed,
/// but the function will panic, if the indices are not unique.
#[deprecated(
    since = "0.40.2",
    note = "Please use `ColumnarAnyBuffer::from_description_and_indices` instead"
)]
pub fn buffer_from_description_and_indices(
    max_rows: usize,
    description: impl Iterator<Item = (u16, BufferDescription)>,
) -> ColumnarBuffer<AnyColumnBuffer> {
    ColumnarAnyBuffer::from_description_and_indices(max_rows, description)
}

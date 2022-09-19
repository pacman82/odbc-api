use std::{collections::HashSet, ffi::c_void};

use odbc_sys::{CDataType, Date, Time, Timestamp};

use crate::{
    columnar_bulk_inserter::BoundInputSlice,
    error::TooLargeBufferSize,
    handles::{CData, CDataMut, HasDataType, StatementRef},
    Bit, DataType, Error,
};

use super::{
    bin_column::BinColumnSliceMut,
    column_with_indicator::{
        OptBitColumn, OptDateColumn, OptF32Column, OptF64Column, OptI16Column, OptI32Column,
        OptI64Column, OptI8Column, OptTimeColumn, OptTimestampColumn, OptU8Column,
    },
    columnar::{ColumnBuffer, ColumnProjections},
    text_column::TextColumnSliceMut,
    BinColumn, BinColumnView, BufferDescription, BufferKind, CharColumn, ColumnarBuffer, Item,
    NullableSlice, NullableSliceMut, TextColumn, TextColumnView, WCharColumn,
};

/// Since buffer shapes are same for all time / timestamps independent of the precision and we do
/// not know the precise SQL type. In order to still be able to bind time / timestamp buffer as
/// input without requiring the user to separately specify the precision, we declare 100 Nano second
/// precision. This was the highest precision still supported by MSSQL in the tests.
const DEFAULT_TIME_PRECISION: i16 = 7;

#[derive(Debug)]
pub enum AnyColumnBuffer {
    /// A buffer for holding both nullable and required binary data.
    Binary(BinColumn),
    /// A buffer for holding both nullable and required text data. Uses the system encoding for
    /// character data.
    Text(CharColumn),
    /// A buffer for holding both nullable and required text data. Uses UTF-16 encoding
    WText(WCharColumn),
    Date(Vec<Date>),
    Time(Vec<Time>),
    Timestamp(Vec<Timestamp>),
    F64(Vec<f64>),
    F32(Vec<f32>),
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    U8(Vec<u8>),
    Bit(Vec<Bit>),
    NullableDate(OptDateColumn),
    NullableTime(OptTimeColumn),
    NullableTimestamp(OptTimestampColumn),
    NullableF64(OptF64Column),
    NullableF32(OptF32Column),
    NullableI8(OptI8Column),
    NullableI16(OptI16Column),
    NullableI32(OptI32Column),
    NullableI64(OptI64Column),
    NullableU8(OptU8Column),
    NullableBit(OptBitColumn),
}

impl AnyColumnBuffer {
    /// Map buffer description to actual buffer.
    pub fn try_from_description(
        max_rows: usize,
        desc: BufferDescription,
    ) -> Result<Self, TooLargeBufferSize> {
        let fallible_allocations = true;
        Self::impl_from_description(max_rows, desc, fallible_allocations)
    }

    /// Map buffer description to actual buffer.
    pub fn from_description(max_rows: usize, desc: BufferDescription) -> Self {
        let fallible_allocations = false;
        Self::impl_from_description(max_rows, desc, fallible_allocations).unwrap()
    }

    /// Map buffer description to actual buffer.
    fn impl_from_description(
        max_rows: usize,
        desc: BufferDescription,
        fallible_allocations: bool,
    ) -> Result<Self, TooLargeBufferSize> {
        let buffer = match (desc.kind, desc.nullable) {
            (BufferKind::Binary { length }, _) => {
                if fallible_allocations {
                    AnyColumnBuffer::Binary(BinColumn::try_new(max_rows as usize, length)?)
                } else {
                    AnyColumnBuffer::Binary(BinColumn::new(max_rows as usize, length))
                }
            }
            (BufferKind::Text { max_str_len }, _) => {
                if fallible_allocations {
                    AnyColumnBuffer::Text(TextColumn::try_new(max_rows as usize, max_str_len)?)
                } else {
                    AnyColumnBuffer::Text(TextColumn::new(max_rows as usize, max_str_len))
                }
            }
            (BufferKind::WText { max_str_len }, _) => {
                if fallible_allocations {
                    AnyColumnBuffer::WText(TextColumn::try_new(max_rows as usize, max_str_len)?)
                } else {
                    AnyColumnBuffer::WText(TextColumn::new(max_rows as usize, max_str_len))
                }
            }
            (BufferKind::Date, false) => {
                AnyColumnBuffer::Date(vec![Date::default(); max_rows as usize])
            }
            (BufferKind::Time, false) => {
                AnyColumnBuffer::Time(vec![Time::default(); max_rows as usize])
            }
            (BufferKind::Timestamp, false) => {
                AnyColumnBuffer::Timestamp(vec![Timestamp::default(); max_rows as usize])
            }
            (BufferKind::F64, false) => {
                AnyColumnBuffer::F64(vec![f64::default(); max_rows as usize])
            }
            (BufferKind::F32, false) => {
                AnyColumnBuffer::F32(vec![f32::default(); max_rows as usize])
            }
            (BufferKind::I8, false) => AnyColumnBuffer::I8(vec![i8::default(); max_rows as usize]),
            (BufferKind::I16, false) => {
                AnyColumnBuffer::I16(vec![i16::default(); max_rows as usize])
            }
            (BufferKind::I32, false) => {
                AnyColumnBuffer::I32(vec![i32::default(); max_rows as usize])
            }
            (BufferKind::I64, false) => {
                AnyColumnBuffer::I64(vec![i64::default(); max_rows as usize])
            }
            (BufferKind::U8, false) => AnyColumnBuffer::U8(vec![u8::default(); max_rows as usize]),
            (BufferKind::Bit, false) => {
                AnyColumnBuffer::Bit(vec![Bit::default(); max_rows as usize])
            }
            (BufferKind::Date, true) => {
                AnyColumnBuffer::NullableDate(OptDateColumn::new(max_rows as usize))
            }
            (BufferKind::Time, true) => {
                AnyColumnBuffer::NullableTime(OptTimeColumn::new(max_rows as usize))
            }
            (BufferKind::Timestamp, true) => {
                AnyColumnBuffer::NullableTimestamp(OptTimestampColumn::new(max_rows as usize))
            }
            (BufferKind::F64, true) => {
                AnyColumnBuffer::NullableF64(OptF64Column::new(max_rows as usize))
            }
            (BufferKind::F32, true) => {
                AnyColumnBuffer::NullableF32(OptF32Column::new(max_rows as usize))
            }
            (BufferKind::I8, true) => {
                AnyColumnBuffer::NullableI8(OptI8Column::new(max_rows as usize))
            }
            (BufferKind::I16, true) => {
                AnyColumnBuffer::NullableI16(OptI16Column::new(max_rows as usize))
            }
            (BufferKind::I32, true) => {
                AnyColumnBuffer::NullableI32(OptI32Column::new(max_rows as usize))
            }
            (BufferKind::I64, true) => {
                AnyColumnBuffer::NullableI64(OptI64Column::new(max_rows as usize))
            }
            (BufferKind::U8, true) => {
                AnyColumnBuffer::NullableU8(OptU8Column::new(max_rows as usize))
            }
            (BufferKind::Bit, true) => {
                AnyColumnBuffer::NullableBit(OptBitColumn::new(max_rows as usize))
            }
        };
        Ok(buffer)
    }

    fn fill_default_slice<T: Default + Copy>(col: &mut [T]) {
        let element = T::default();
        for item in col {
            *item = element;
        }
    }

    fn inner_cdata(&self) -> &dyn CData {
        match self {
            AnyColumnBuffer::Binary(col) => col,
            AnyColumnBuffer::Text(col) => col,
            AnyColumnBuffer::WText(col) => col,
            AnyColumnBuffer::F64(col) => col,
            AnyColumnBuffer::F32(col) => col,
            AnyColumnBuffer::Date(col) => col,
            AnyColumnBuffer::Time(col) => col,
            AnyColumnBuffer::Timestamp(col) => col,
            AnyColumnBuffer::I8(col) => col,
            AnyColumnBuffer::I16(col) => col,
            AnyColumnBuffer::I32(col) => col,
            AnyColumnBuffer::I64(col) => col,
            AnyColumnBuffer::Bit(col) => col,
            AnyColumnBuffer::U8(col) => col,
            AnyColumnBuffer::NullableF64(col) => col,
            AnyColumnBuffer::NullableF32(col) => col,
            AnyColumnBuffer::NullableDate(col) => col,
            AnyColumnBuffer::NullableTime(col) => col,
            AnyColumnBuffer::NullableTimestamp(col) => col,
            AnyColumnBuffer::NullableI8(col) => col,
            AnyColumnBuffer::NullableI16(col) => col,
            AnyColumnBuffer::NullableI32(col) => col,
            AnyColumnBuffer::NullableI64(col) => col,
            AnyColumnBuffer::NullableBit(col) => col,
            AnyColumnBuffer::NullableU8(col) => col,
        }
    }

    fn inner_cdata_mut(&mut self) -> &mut dyn CDataMut {
        match self {
            AnyColumnBuffer::Binary(col) => col,
            AnyColumnBuffer::Text(col) => col,
            AnyColumnBuffer::WText(col) => col,
            AnyColumnBuffer::F64(col) => col,
            AnyColumnBuffer::F32(col) => col,
            AnyColumnBuffer::Date(col) => col,
            AnyColumnBuffer::Time(col) => col,
            AnyColumnBuffer::Timestamp(col) => col,
            AnyColumnBuffer::I8(col) => col,
            AnyColumnBuffer::I16(col) => col,
            AnyColumnBuffer::I32(col) => col,
            AnyColumnBuffer::I64(col) => col,
            AnyColumnBuffer::Bit(col) => col,
            AnyColumnBuffer::U8(col) => col,
            AnyColumnBuffer::NullableF64(col) => col,
            AnyColumnBuffer::NullableF32(col) => col,
            AnyColumnBuffer::NullableDate(col) => col,
            AnyColumnBuffer::NullableTime(col) => col,
            AnyColumnBuffer::NullableTimestamp(col) => col,
            AnyColumnBuffer::NullableI8(col) => col,
            AnyColumnBuffer::NullableI16(col) => col,
            AnyColumnBuffer::NullableI32(col) => col,
            AnyColumnBuffer::NullableI64(col) => col,
            AnyColumnBuffer::NullableBit(col) => col,
            AnyColumnBuffer::NullableU8(col) => col,
        }
    }
}

unsafe impl CData for AnyColumnBuffer {
    fn cdata_type(&self) -> CDataType {
        self.inner_cdata().cdata_type()
    }

    fn indicator_ptr(&self) -> *const isize {
        self.inner_cdata().indicator_ptr()
    }

    fn value_ptr(&self) -> *const c_void {
        self.inner_cdata().value_ptr()
    }

    fn buffer_length(&self) -> isize {
        self.inner_cdata().buffer_length()
    }
}

unsafe impl CDataMut for AnyColumnBuffer {
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        self.inner_cdata_mut().mut_indicator_ptr()
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.inner_cdata_mut().mut_value_ptr()
    }
}

impl HasDataType for AnyColumnBuffer {
    fn data_type(&self) -> DataType {
        match self {
            AnyColumnBuffer::Binary(col) => col.data_type(),
            AnyColumnBuffer::Text(col) => col.data_type(),
            AnyColumnBuffer::WText(col) => col.data_type(),
            AnyColumnBuffer::Date(_) | AnyColumnBuffer::NullableDate(_) => DataType::Date,
            AnyColumnBuffer::Time(_) | AnyColumnBuffer::NullableTime(_) => DataType::Time {
                precision: DEFAULT_TIME_PRECISION,
            },
            AnyColumnBuffer::Timestamp(_) | AnyColumnBuffer::NullableTimestamp(_) => {
                DataType::Timestamp {
                    precision: DEFAULT_TIME_PRECISION,
                }
            }
            AnyColumnBuffer::F64(_) | AnyColumnBuffer::NullableF64(_) => DataType::Double,
            AnyColumnBuffer::F32(_) | AnyColumnBuffer::NullableF32(_) => DataType::Real,
            AnyColumnBuffer::I8(_) | AnyColumnBuffer::NullableI8(_) => DataType::TinyInt,
            AnyColumnBuffer::I16(_) | AnyColumnBuffer::NullableI16(_) => DataType::SmallInt,
            AnyColumnBuffer::I32(_) | AnyColumnBuffer::NullableI32(_) => DataType::Integer,
            AnyColumnBuffer::I64(_) | AnyColumnBuffer::NullableI64(_) => DataType::BigInt,
            // Few databases support unsigned types, binding U8 as tiny int might lead to weird
            // stuff if the database has type is signed. I guess. Let's bind it as SmallInt by
            // default, just to be on the safe side.
            AnyColumnBuffer::U8(_) | AnyColumnBuffer::NullableU8(_) => DataType::SmallInt,
            AnyColumnBuffer::Bit(_) | AnyColumnBuffer::NullableBit(_) => DataType::Bit,
        }
    }
}

/// Flexible columnar buffer implementation. Bind this to a cursor to fetch values in bulk, or pass
/// this as a parameter to a statement, to submit many parameters at once.
pub type ColumnarAnyBuffer = ColumnarBuffer<AnyColumnBuffer>;

impl ColumnarAnyBuffer {
    /// Allocates a [`ColumnarBuffer`] fitting the buffer descriptions.
    pub fn from_description(
        capacity: usize,
        descs: impl IntoIterator<Item = BufferDescription>,
    ) -> Self {
        let mut column_index = 0;
        let columns = descs
            .into_iter()
            .map(move |desc| {
                let buffer = AnyColumnBuffer::from_description(capacity, desc);
                column_index += 1;
                (column_index, buffer)
            })
            .collect();
        unsafe { ColumnarBuffer::new_unchecked(capacity, columns) }
    }

    /// Allocates a [`ColumnarBuffer`] fitting the buffer descriptions. If not enough memory is
    /// available to allocate the buffers this function fails with
    /// [`Error::TooLargeColumnBufferSize`]. This function is slower than [`Self::from_description`]
    /// which would just panic if not enough memory is available for allocation.
    pub fn try_from_description(
        capacity: usize,
        descs: impl Iterator<Item = BufferDescription>,
    ) -> Result<Self, Error> {
        let mut column_index = 0;
        let columns = descs
            .map(move |desc| {
                let buffer = AnyColumnBuffer::try_from_description(capacity, desc)
                    .map_err(|source| source.add_context(column_index))?;
                column_index += 1;
                Ok((column_index, buffer))
            })
            .collect::<Result<_, _>>()?;
        Ok(unsafe { ColumnarBuffer::new_unchecked(capacity, columns) })
    }

    /// Allows you to pass the buffer descriptions together with a one based column index referring
    /// the column, the buffer is supposed to bind to. This allows you also to ignore columns in a
    /// result set, by not binding them at all. There is no restriction on the order of column
    /// indices passed, but the function will panic, if the indices are not unique.
    pub fn from_description_and_indices(
        max_rows: usize,
        description: impl Iterator<Item = (u16, BufferDescription)>,
    ) -> ColumnarBuffer<AnyColumnBuffer> {
        let columns: Vec<_> = description
            .map(|(col_index, buffer_desc)| {
                (
                    col_index,
                    AnyColumnBuffer::from_description(max_rows, buffer_desc),
                )
            })
            .collect();

        // Assert uniqueness of indices
        let mut indices = HashSet::new();
        if columns
            .iter()
            .any(move |&(col_index, _)| !indices.insert(col_index))
        {
            panic!("Column indices must be unique.")
        }

        ColumnarBuffer::new(columns)
    }
}

/// A borrowed view on the valid rows in a column of a [`crate::buffers::ColumnarBuffer`].
///
/// For columns of fixed size types, which are guaranteed to not contain null, a direct access to
/// the slice is offered. Buffers over nullable columns can be accessed via an iterator over
/// options.
#[derive(Debug, Clone, Copy)]
pub enum AnyColumnView<'a> {
    /// Nullable character data in the system encoding.
    Text(TextColumnView<'a, u8>),
    /// Nullable character data encoded in UTF-16.
    WText(TextColumnView<'a, u16>),
    Binary(BinColumnView<'a>),
    Date(&'a [Date]),
    Time(&'a [Time]),
    Timestamp(&'a [Timestamp]),
    F64(&'a [f64]),
    F32(&'a [f32]),
    I8(&'a [i8]),
    I16(&'a [i16]),
    I32(&'a [i32]),
    I64(&'a [i64]),
    U8(&'a [u8]),
    Bit(&'a [Bit]),
    NullableDate(NullableSlice<'a, Date>),
    NullableTime(NullableSlice<'a, Time>),
    NullableTimestamp(NullableSlice<'a, Timestamp>),
    NullableF64(NullableSlice<'a, f64>),
    NullableF32(NullableSlice<'a, f32>),
    NullableI8(NullableSlice<'a, i8>),
    NullableI16(NullableSlice<'a, i16>),
    NullableI32(NullableSlice<'a, i32>),
    NullableI64(NullableSlice<'a, i64>),
    NullableU8(NullableSlice<'a, u8>),
    NullableBit(NullableSlice<'a, Bit>),
}

impl<'a> AnyColumnView<'a> {
    /// This method is useful if you expect the variant to be [`AnyColumnView::Text`]. It allows you
    /// to unwrap the inner column view without explictly matching it.
    pub fn as_text_view(self) -> Option<TextColumnView<'a, u8>> {
        if let Self::Text(view) = self {
            Some(view)
        } else {
            None
        }
    }

    /// This method is useful if you expect the variant to be [`AnyColumnView::WText`]. It allows
    /// you to unwrap the inner column view without explictly matching it.
    pub fn as_w_text_view(self) -> Option<TextColumnView<'a, u16>> {
        if let Self::WText(view) = self {
            Some(view)
        } else {
            None
        }
    }

    /// This method is useful if you expect the variant to be [`AnyColumnView::Binary`]. It allows
    /// you to unwrap the inner column view without explictly matching it.
    pub fn as_bin_view(self) -> Option<BinColumnView<'a>> {
        if let Self::Binary(view) = self {
            Some(view)
        } else {
            None
        }
    }

    /// Extract the array type from an [`AnyColumnView`].
    pub fn as_slice<I: Item>(self) -> Option<&'a [I]> {
        I::as_slice(self)
    }

    /// Extract the typed nullable buffer from an [`AnyColumnView`].
    pub fn as_nullable_slice<I: Item>(self) -> Option<NullableSlice<'a, I>> {
        I::as_nullable_slice(self)
    }
}

unsafe impl<'a> BoundInputSlice<'a> for AnyColumnBuffer {
    type SliceMut = AnyColumnSliceMut<'a>;

    unsafe fn as_view_mut(
        &'a mut self,
        parameter_index: u16,
        stmt: StatementRef<'a>,
    ) -> Self::SliceMut {
        let num_rows = self.capacity();
        match self {
            AnyColumnBuffer::Binary(column) => {
                AnyColumnSliceMut::Binary(column.as_view_mut(parameter_index, stmt))
            }
            AnyColumnBuffer::Text(column) => {
                AnyColumnSliceMut::Text(column.as_view_mut(parameter_index, stmt))
            }
            AnyColumnBuffer::WText(column) => {
                AnyColumnSliceMut::WText(column.as_view_mut(parameter_index, stmt))
            }
            AnyColumnBuffer::Date(column) => AnyColumnSliceMut::Date(column),
            AnyColumnBuffer::Time(column) => AnyColumnSliceMut::Time(column),
            AnyColumnBuffer::Timestamp(column) => AnyColumnSliceMut::Timestamp(column),
            AnyColumnBuffer::F64(column) => AnyColumnSliceMut::F64(column),
            AnyColumnBuffer::F32(column) => AnyColumnSliceMut::F32(column),
            AnyColumnBuffer::I8(column) => AnyColumnSliceMut::I8(column),
            AnyColumnBuffer::I16(column) => AnyColumnSliceMut::I16(column),
            AnyColumnBuffer::I32(column) => AnyColumnSliceMut::I32(column),
            AnyColumnBuffer::I64(column) => AnyColumnSliceMut::I64(column),
            AnyColumnBuffer::U8(column) => AnyColumnSliceMut::U8(column),
            AnyColumnBuffer::Bit(column) => AnyColumnSliceMut::Bit(column),
            AnyColumnBuffer::NullableDate(column) => {
                AnyColumnSliceMut::NullableDate(column.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableTime(column) => {
                AnyColumnSliceMut::NullableTime(column.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableTimestamp(column) => {
                AnyColumnSliceMut::NullableTimestamp(column.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableF64(column) => {
                AnyColumnSliceMut::NullableF64(column.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableF32(column) => {
                AnyColumnSliceMut::NullableF32(column.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableI8(column) => {
                AnyColumnSliceMut::NullableI8(column.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableI16(column) => {
                AnyColumnSliceMut::NullableI16(column.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableI32(column) => {
                AnyColumnSliceMut::NullableI32(column.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableI64(column) => {
                AnyColumnSliceMut::NullableI64(column.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableU8(column) => {
                AnyColumnSliceMut::NullableU8(column.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableBit(column) => {
                AnyColumnSliceMut::NullableBit(column.writer_n(num_rows))
            }
        }
    }
}

/// A mutable slice of an input buffer, with runtime type information. Edit values in this slice in
/// order to send parameters in bulk to a database.
pub enum AnyColumnSliceMut<'a> {
    Text(TextColumnSliceMut<'a, u8>),
    /// Nullable character data encoded in UTF-16.
    WText(TextColumnSliceMut<'a, u16>),
    Binary(BinColumnSliceMut<'a>),
    Date(&'a mut [Date]),
    Time(&'a mut [Time]),
    Timestamp(&'a mut [Timestamp]),
    F64(&'a mut [f64]),
    F32(&'a mut [f32]),
    I8(&'a mut [i8]),
    I16(&'a mut [i16]),
    I32(&'a mut [i32]),
    I64(&'a mut [i64]),
    U8(&'a mut [u8]),
    Bit(&'a mut [Bit]),
    NullableDate(NullableSliceMut<'a, Date>),
    NullableTime(NullableSliceMut<'a, Time>),
    NullableTimestamp(NullableSliceMut<'a, Timestamp>),
    NullableF64(NullableSliceMut<'a, f64>),
    NullableF32(NullableSliceMut<'a, f32>),
    NullableI8(NullableSliceMut<'a, i8>),
    NullableI16(NullableSliceMut<'a, i16>),
    NullableI32(NullableSliceMut<'a, i32>),
    NullableI64(NullableSliceMut<'a, i64>),
    NullableU8(NullableSliceMut<'a, u8>),
    NullableBit(NullableSliceMut<'a, Bit>),
}

impl<'a> AnyColumnSliceMut<'a> {
    /// This method is useful if you expect the variant to be [`AnyColumnSliceMut::Binary`]. It
    /// allows to you unwrap the inner column view without explictly matching it.
    pub fn as_bin_view(self) -> Option<BinColumnSliceMut<'a>> {
        if let Self::Binary(view) = self {
            Some(view)
        } else {
            None
        }
    }

    /// This method is useful if you expect the variant to be [`AnyColumnSliceMut::Text`]. It allows
    /// to you unwrap the inner column view without explictly matching it.
    pub fn as_text_view(self) -> Option<TextColumnSliceMut<'a, u8>> {
        if let Self::Text(view) = self {
            Some(view)
        } else {
            None
        }
    }

    /// This method is useful if you expect the variant to be [`AnyColumnSliceMut::WText`]. It
    /// allows you to unwrap the inner column view without explictly matching it.
    pub fn as_w_text_view(self) -> Option<TextColumnSliceMut<'a, u16>> {
        if let Self::WText(view) = self {
            Some(view)
        } else {
            None
        }
    }

    /// Extract the array type from an [`AnyColumnSliceMut`].
    pub fn as_slice<I: Item>(self) -> Option<&'a mut [I]> {
        I::as_slice_mut(self)
    }

    /// Extract the typed nullable buffer from an [`AnyColumnSliceMut`].
    pub fn as_nullable_slice<I: Item>(self) -> Option<NullableSliceMut<'a, I>> {
        I::as_nullable_slice_mut(self)
    }
}

unsafe impl<'a> ColumnProjections<'a> for AnyColumnBuffer {
    type View = AnyColumnView<'a>;
}

unsafe impl ColumnBuffer for AnyColumnBuffer {
    fn capacity(&self) -> usize {
        match self {
            AnyColumnBuffer::Binary(col) => col.capacity(),
            AnyColumnBuffer::Text(col) => col.capacity(),
            AnyColumnBuffer::WText(col) => col.capacity(),
            AnyColumnBuffer::Date(col) => col.capacity(),
            AnyColumnBuffer::Time(col) => col.capacity(),
            AnyColumnBuffer::Timestamp(col) => col.capacity(),
            AnyColumnBuffer::F64(col) => col.capacity(),
            AnyColumnBuffer::F32(col) => col.capacity(),
            AnyColumnBuffer::I8(col) => col.capacity(),
            AnyColumnBuffer::I16(col) => col.capacity(),
            AnyColumnBuffer::I32(col) => col.capacity(),
            AnyColumnBuffer::I64(col) => col.capacity(),
            AnyColumnBuffer::U8(col) => col.capacity(),
            AnyColumnBuffer::Bit(col) => col.capacity(),
            AnyColumnBuffer::NullableDate(col) => col.capacity(),
            AnyColumnBuffer::NullableTime(col) => col.capacity(),
            AnyColumnBuffer::NullableTimestamp(col) => col.capacity(),
            AnyColumnBuffer::NullableF64(col) => col.capacity(),
            AnyColumnBuffer::NullableF32(col) => col.capacity(),
            AnyColumnBuffer::NullableI8(col) => col.capacity(),
            AnyColumnBuffer::NullableI16(col) => col.capacity(),
            AnyColumnBuffer::NullableI32(col) => col.capacity(),
            AnyColumnBuffer::NullableI64(col) => col.capacity(),
            AnyColumnBuffer::NullableU8(col) => col.capacity(),
            AnyColumnBuffer::NullableBit(col) => col.capacity(),
        }
    }

    fn view(&self, valid_rows: usize) -> AnyColumnView {
        match self {
            AnyColumnBuffer::Binary(col) => AnyColumnView::Binary(col.view(valid_rows)),
            AnyColumnBuffer::Text(col) => AnyColumnView::Text(col.view(valid_rows)),
            AnyColumnBuffer::WText(col) => AnyColumnView::WText(col.view(valid_rows)),
            AnyColumnBuffer::Date(col) => AnyColumnView::Date(&col[0..valid_rows]),
            AnyColumnBuffer::Time(col) => AnyColumnView::Time(&col[0..valid_rows]),
            AnyColumnBuffer::Timestamp(col) => AnyColumnView::Timestamp(&col[0..valid_rows]),
            AnyColumnBuffer::F64(col) => AnyColumnView::F64(&col[0..valid_rows]),
            AnyColumnBuffer::F32(col) => AnyColumnView::F32(&col[0..valid_rows]),
            AnyColumnBuffer::I8(col) => AnyColumnView::I8(&col[0..valid_rows]),
            AnyColumnBuffer::I16(col) => AnyColumnView::I16(&col[0..valid_rows]),
            AnyColumnBuffer::I32(col) => AnyColumnView::I32(&col[0..valid_rows]),
            AnyColumnBuffer::I64(col) => AnyColumnView::I64(&col[0..valid_rows]),
            AnyColumnBuffer::U8(col) => AnyColumnView::U8(&col[0..valid_rows]),
            AnyColumnBuffer::Bit(col) => AnyColumnView::Bit(&col[0..valid_rows]),
            AnyColumnBuffer::NullableDate(col) => AnyColumnView::NullableDate(col.iter(valid_rows)),
            AnyColumnBuffer::NullableTime(col) => AnyColumnView::NullableTime(col.iter(valid_rows)),
            AnyColumnBuffer::NullableTimestamp(col) => {
                AnyColumnView::NullableTimestamp(col.iter(valid_rows))
            }
            AnyColumnBuffer::NullableF64(col) => AnyColumnView::NullableF64(col.iter(valid_rows)),
            AnyColumnBuffer::NullableF32(col) => AnyColumnView::NullableF32(col.iter(valid_rows)),
            AnyColumnBuffer::NullableI8(col) => AnyColumnView::NullableI8(col.iter(valid_rows)),
            AnyColumnBuffer::NullableI16(col) => AnyColumnView::NullableI16(col.iter(valid_rows)),
            AnyColumnBuffer::NullableI32(col) => AnyColumnView::NullableI32(col.iter(valid_rows)),
            AnyColumnBuffer::NullableI64(col) => AnyColumnView::NullableI64(col.iter(valid_rows)),
            AnyColumnBuffer::NullableU8(col) => AnyColumnView::NullableU8(col.iter(valid_rows)),
            AnyColumnBuffer::NullableBit(col) => AnyColumnView::NullableBit(col.iter(valid_rows)),
        }
    }

    /// Fills the column with the default representation of values, between `from` and `to` index.
    fn fill_default(&mut self, from: usize, to: usize) {
        match self {
            AnyColumnBuffer::Binary(col) => col.fill_null(from, to),
            AnyColumnBuffer::Text(col) => col.fill_null(from, to),
            AnyColumnBuffer::WText(col) => col.fill_null(from, to),
            AnyColumnBuffer::Date(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyColumnBuffer::Time(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyColumnBuffer::Timestamp(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyColumnBuffer::F64(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyColumnBuffer::F32(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyColumnBuffer::I8(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyColumnBuffer::I16(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyColumnBuffer::I32(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyColumnBuffer::I64(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyColumnBuffer::U8(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyColumnBuffer::Bit(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyColumnBuffer::NullableDate(col) => col.fill_null(from, to),
            AnyColumnBuffer::NullableTime(col) => col.fill_null(from, to),
            AnyColumnBuffer::NullableTimestamp(col) => col.fill_null(from, to),
            AnyColumnBuffer::NullableF64(col) => col.fill_null(from, to),
            AnyColumnBuffer::NullableF32(col) => col.fill_null(from, to),
            AnyColumnBuffer::NullableI8(col) => col.fill_null(from, to),
            AnyColumnBuffer::NullableI16(col) => col.fill_null(from, to),
            AnyColumnBuffer::NullableI32(col) => col.fill_null(from, to),
            AnyColumnBuffer::NullableI64(col) => col.fill_null(from, to),
            AnyColumnBuffer::NullableU8(col) => col.fill_null(from, to),
            AnyColumnBuffer::NullableBit(col) => col.fill_null(from, to),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffers::{ColumnBuffer, AnyColumnSliceMut, AnyColumnView};

    use super::AnyColumnBuffer;

    #[test]
    fn slice_should_only_contain_part_of_the_buffer() {
        let buffer = AnyColumnBuffer::I32(vec![1,2,3]);

        let view = buffer.view(2);

        assert_eq!(Some([1,2].as_slice()), view.as_slice::<i32>());
    }

    #[test]
    fn slice_should_be_none_if_types_mismatch() {
        let buffer = [1,2,3];
        let view = AnyColumnView::I32(&buffer);
        assert_eq!(None, view.as_slice::<i16>());
    }

    #[test]
    fn slice_mut_should_be_none_if_types_mismatch() {
        let mut buffer = [1,2,3];
        let view = AnyColumnSliceMut::I32(&mut buffer);
        assert_eq!(None, view.as_slice::<i16>());
    }

    #[test]
    fn nullable_slice_should_be_none_if_buffer_is_non_nullable() {
        let buffer = [1,2,3];
        let view = AnyColumnView::I32(&buffer);
        assert!(view.as_nullable_slice::<i32>().is_none());
    }

    #[test]
    fn nullable_slice_mut_should_be_none_if_buffer_is_non_nullable() {
        let mut buffer = [1,2,3];
        let view = AnyColumnSliceMut::I32(&mut buffer);
        assert!(view.as_nullable_slice::<i32>().is_none());
    }
}
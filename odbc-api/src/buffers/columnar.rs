use std::{collections::HashSet, ffi::c_void};

use crate::{
    fixed_sized::Bit,
    handles::{CData, CDataMut, HasDataType, Statement},
    Cursor, DataType, Error, ParameterRefCollection, RowSetBuffer,
};

use super::{
    bin_column::{BinColumn, BinColumnIt, BinColumnWriter},
    column_with_indicator::{
        NullableSlice, NullableSliceMut, OptBitColumn, OptDateColumn, OptF32Column, OptF64Column,
        OptI16Column, OptI32Column, OptI64Column, OptI8Column, OptTimeColumn, OptTimestampColumn,
        OptU8Column,
    },
    text_column::{TextColumn, TextColumnIt, TextColumnWriter},
    BufferDescription, BufferKind, CharColumn, WCharColumn,
};

use odbc_sys::{CDataType, Date, Time, Timestamp};

/// Since buffer shapes are same for all time / timestamps independent of the precision and we do
/// not know the precise SQL type. In order to still be able to bind time / timestamp buffer as
/// input without requiring the user to separately specify the precision, we declare 100 Nano second
/// precision. This was the highest precision still supported by MSSQL in the tests.
const DEFAULT_TIME_PRECISION: i16 = 7;

/// A borrowed view on the valid rows in a column of a [`crate::buffers::ColumnarRowSet`].
///
/// For columns of fixed size types, which are guaranteed to not contain null, a direct access to
/// the slice is offered. Buffers over nullable columns can be accessed via an iterator over
/// options.
#[derive(Debug)]
pub enum AnyColumnView<'a> {
    /// Nullable character data in the system encoding.
    Text(TextColumnIt<'a, u8>),
    /// Nullable character data encoded in UTF-16.
    WText(TextColumnIt<'a, u16>),
    Binary(BinColumnIt<'a>),
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

/// A mutable borrowed view on the valid rows in a column of a [`crate::buffers::ColumnarRowSet`].
///
/// For columns of fixed size types, which are guaranteed to not contain null, a direct access to
/// the slice is offered. Buffers over nullable columns can be accessed via an iterator over
/// options.
#[derive(Debug)]
pub enum AnyColumnViewMut<'a> {
    /// Nullable character data in system encoding.
    Text(TextColumnWriter<'a, u8>),
    /// Nullable character data encoded in UTF-16.
    WText(TextColumnWriter<'a, u16>),
    Binary(BinColumnWriter<'a>),
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
    pub fn new(max_rows: usize, desc: BufferDescription) -> Self {
        match (desc.kind, desc.nullable) {
            (BufferKind::Binary { length }, _) => {
                AnyColumnBuffer::Binary(BinColumn::new(max_rows as usize, length))
            }
            (BufferKind::Text { max_str_len }, _) => {
                AnyColumnBuffer::Text(TextColumn::new(max_rows as usize, max_str_len))
            }
            (BufferKind::WText { max_str_len }, _) => {
                AnyColumnBuffer::WText(TextColumn::new(max_rows as usize, max_str_len))
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
        }
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

/// Convinience function allocating a ColumnarRowSet fitting the buffer descriptions.
pub fn default_buffer(
    capacity: usize,
    descs: impl Iterator<Item = BufferDescription>,
) -> ColumnarRowSet<AnyColumnBuffer> {
    ColumnarRowSet::from_buffer_descriptions(capacity, descs)
}

/// A columnar buffer intended to be bound with [crate::Cursor::bind_buffer] in order to obtain
/// results from a cursor.
///
/// This buffer is designed to be versatile. It supports a wide variety of usage scenarios. It is
/// efficient in retrieving data, but expensive to allocate, as columns are allocated separately.
/// This is required in order to efficiently allow for rebinding columns, if this buffer is used to
/// provide array input parameters those maximum size is not known in advance.
///
/// Most applications should find the overhead negligible, especially if instances are reused.
pub struct ColumnarRowSet<C> {
    /// Use a box, so it is safe for a cursor to take ownership of this buffer.
    num_rows: Box<usize>,
    /// aka: batch size, row array size
    max_rows: usize,
    /// Column index and bound buffer
    columns: Vec<(u16, C)>,
}

/// A buffer able to be used together with [`ColumnBuffer`].
///
/// # Safety
///
/// Views must not allow access to unintialized / invalid rows.
pub unsafe trait ColumnBuffer:
    for<'a> ColumnProjections<'a> + CDataMut + HasDataType
{
    fn from_description(capacity: usize, desc: BufferDescription) -> Self;

    /// # Safety
    ///
    /// Underlying buffer may not know how many elements have been written to it by the last ODBC
    /// function call. So we tell it how many, and get a save to use view in Return. Specifying an
    /// erroneous value for `valid_rows`, may therfore result in the construced view giving us
    /// access to invalid rows in a safe abstraction, which of course would be a Bug.
    unsafe fn view(&self, valid_rows: usize) -> <Self as ColumnProjections<'_>>::View;

    /// # Safety
    ///
    /// `valid_rows` must be valid, otherwise the safe abstraction would provide access to invalid
    /// memory.
    unsafe fn view_mut(&mut self, valid_rows: usize) -> <Self as ColumnProjections<'_>>::ViewMut;

    /// Fills the column with the default representation of values, between `from` and `to` index.
    fn fill_default(&mut self, from: usize, to: usize);
}

unsafe impl ColumnBuffer for AnyColumnBuffer {
    fn from_description(capacity: usize, desc: BufferDescription) -> Self {
        Self::new(capacity, desc)
    }

    unsafe fn view(&self, valid_rows: usize) -> AnyColumnView {
        match self {
            AnyColumnBuffer::Binary(col) => AnyColumnView::Binary(col.iter(valid_rows)),
            AnyColumnBuffer::Text(col) => AnyColumnView::Text(col.iter(valid_rows)),
            AnyColumnBuffer::WText(col) => AnyColumnView::WText(col.iter(valid_rows)),
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

    unsafe fn view_mut(&mut self, num_rows: usize) -> AnyColumnViewMut<'_> {
        match self {
            AnyColumnBuffer::Text(col) => AnyColumnViewMut::Text(col.writer_n(num_rows)),
            AnyColumnBuffer::WText(col) => AnyColumnViewMut::WText(col.writer_n(num_rows)),
            AnyColumnBuffer::Binary(col) => AnyColumnViewMut::Binary(col.writer_n(num_rows)),
            AnyColumnBuffer::Date(col) => AnyColumnViewMut::Date(&mut col[0..num_rows]),
            AnyColumnBuffer::Time(col) => AnyColumnViewMut::Time(&mut col[0..num_rows]),
            AnyColumnBuffer::Timestamp(col) => AnyColumnViewMut::Timestamp(&mut col[0..num_rows]),
            AnyColumnBuffer::F64(col) => AnyColumnViewMut::F64(&mut col[0..num_rows]),
            AnyColumnBuffer::F32(col) => AnyColumnViewMut::F32(&mut col[0..num_rows]),
            AnyColumnBuffer::I8(col) => AnyColumnViewMut::I8(&mut col[0..num_rows]),
            AnyColumnBuffer::I16(col) => AnyColumnViewMut::I16(&mut col[0..num_rows]),
            AnyColumnBuffer::I32(col) => AnyColumnViewMut::I32(&mut col[0..num_rows]),
            AnyColumnBuffer::I64(col) => AnyColumnViewMut::I64(&mut col[0..num_rows]),
            AnyColumnBuffer::U8(col) => AnyColumnViewMut::U8(&mut col[0..num_rows]),
            AnyColumnBuffer::Bit(col) => AnyColumnViewMut::Bit(&mut col[0..num_rows]),
            AnyColumnBuffer::NullableDate(col) => {
                AnyColumnViewMut::NullableDate(col.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableTime(col) => {
                AnyColumnViewMut::NullableTime(col.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableTimestamp(col) => {
                AnyColumnViewMut::NullableTimestamp(col.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableF64(col) => {
                AnyColumnViewMut::NullableF64(col.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableF32(col) => {
                AnyColumnViewMut::NullableF32(col.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableI8(col) => {
                AnyColumnViewMut::NullableI8(col.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableI16(col) => {
                AnyColumnViewMut::NullableI16(col.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableI32(col) => {
                AnyColumnViewMut::NullableI32(col.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableI64(col) => {
                AnyColumnViewMut::NullableI64(col.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableU8(col) => {
                AnyColumnViewMut::NullableU8(col.writer_n(num_rows))
            }
            AnyColumnBuffer::NullableBit(col) => {
                AnyColumnViewMut::NullableBit(col.writer_n(num_rows))
            }
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

/// Projections for ColumnBuffers, allowing for reading writing data while bound as a rowset or
/// parameter buffer without invalidating invariants of the type.
///
/// Intended as part for the ColumnBuffer trait. Currently seperated to allow to compile without
/// GAT.
///
/// # Safety
///
/// View may not allow access to invalid rows.
pub unsafe trait ColumnProjections<'a> {
    /// Immutable view on the column data. Used in safe abstractions. User must not be able to
    /// access uninitialized or invalid memory of the buffer through this interface.
    type View;

    /// Used to gain access to the buffer, if bound as a parameter for inserting.
    type ViewMut;
}

unsafe impl<'a> ColumnProjections<'a> for AnyColumnBuffer {
    type View = AnyColumnView<'a>;

    type ViewMut = AnyColumnViewMut<'a>;
}

impl<C: ColumnBuffer> ColumnarRowSet<C> {
    /// Allocates for each buffer description a buffer large enough to hold `max_rows`.
    pub fn from_buffer_descriptions(
        max_rows: usize,
        description: impl Iterator<Item = BufferDescription>,
    ) -> Self {
        let mut column_index = 0;
        let columns = description
            .map(move |desc| {
                column_index += 1;
                (column_index, C::from_description(max_rows, desc))
            })
            .collect();
        ColumnarRowSet {
            num_rows: Box::new(0),
            max_rows,
            columns,
        }
    }

    /// Allows you to pass the buffer descriptions together with a one based column index referring
    /// the column, the buffer is supposed to bind to. This allows you also to ignore columns in a
    /// result set, by not binding them at all. There is no restriction on the order of column
    /// indices passed, but the function will panic, if the indices are not unique.
    pub fn with_column_indices(
        max_rows: usize,
        description: impl Iterator<Item = (u16, BufferDescription)>,
    ) -> Self {
        let columns: Vec<_> = description
            .map(|(col_index, buffer_desc)| (col_index, C::from_description(max_rows, buffer_desc)))
            .collect();

        // Assert uniqueness of indices
        let mut indices = HashSet::new();
        if columns
            .iter()
            .any(move |&(col_index, _)| !indices.insert(col_index))
        {
            panic!("Column indices must be unique.")
        }

        ColumnarRowSet {
            num_rows: Box::new(0),
            max_rows,
            columns,
        }
    }

    /// Number of valid rows in the buffer.
    pub fn num_rows(&self) -> usize {
        *self.num_rows
    }

    /// Use this method to gain read access to the actual column data.
    ///
    /// # Parameters
    ///
    /// * `buffer_index`: Please note that the buffer index is not identical to the ODBC column
    ///   index. For once it is zero based. It also indexes the buffer bound, and not the columns of
    ///   the output result set. This is important, because not every column needs to be bound. Some
    ///   columns may simply be ignored. That being said, if every column of the output is bound in
    ///   the buffer, in the same order in which they are enumerated in the result set, the
    ///   relationship between column index and buffer index is `buffer_index = column_index - 1`.
    pub fn column(&self, buffer_index: usize) -> <C as ColumnProjections<'_>>::View {
        unsafe { self.columns[buffer_index].1.view(*self.num_rows) }
    }

    /// Use this method to gain write access to the actual column data.
    ///
    /// # Parameters
    ///
    /// * `buffer_index`: Please note that the buffer index is not identical to the ODBC column
    ///   index. For once it is zero based. It also indexes the buffer bound, and not the columns of
    ///   the output result set. This is important, because not every column needs to be bound. Some
    ///   columns may simply be ignored. That being said, if every column of the output is bound in
    ///   the buffer, in the same order in which they are enumerated in the result set, the
    ///   relationship between column index and buffer index is `buffer_index = column_index - 1`.
    ///
    /// # Example
    ///
    /// This method is intend to be called if using [`ColumnarRowSet`] for column wise bulk inserts.
    ///
    /// ```no_run
    /// use odbc_api::{
    ///     Connection, Error, IntoParameter,
    ///     buffers::{BufferDescription, BufferKind, AnyColumnViewMut, default_buffer}
    /// };
    ///
    /// fn insert_birth_years(conn: &Connection, names: &[&str], years: &[i16])
    ///     -> Result<(), Error>
    /// {
    ///
    ///     // All columns must have equal length.
    ///     assert_eq!(names.len(), years.len());
    ///
    ///     // Create a columnar buffer which fits the input parameters.
    ///     let buffer_description = [
    ///         BufferDescription {
    ///             kind: BufferKind::Text { max_str_len: 255 },
    ///             nullable: false,
    ///         },
    ///         BufferDescription {
    ///             kind: BufferKind::I16,
    ///             nullable: false,
    ///         },
    ///     ];
    ///     let mut buffer = default_buffer(names.len(), buffer_description.iter().copied());
    ///
    ///     // Fill the buffer with values column by column
    ///     match buffer.column_mut(0) {
    ///         AnyColumnViewMut::Text(mut col) => {
    ///             col.write(names.iter().map(|s| Some(s.as_bytes())))
    ///         }
    ///         _ => panic!("We know the name column to hold text.")
    ///     }
    ///
    ///     match buffer.column_mut(1) {
    ///         AnyColumnViewMut::I16(mut col) => {
    ///             col.copy_from_slice(years)
    ///         }
    ///         _ => panic!("We know the year column to hold i16.")
    ///     }
    ///
    ///     conn.execute(
    ///         "INSERT INTO Birthdays (name, year) VALUES (?, ?)",
    ///         &buffer
    ///     )?;
    ///     Ok(())
    /// }
    /// ```
    pub fn column_mut(&mut self, buffer_index: usize) -> <C as ColumnProjections<'_>>::ViewMut {
        unsafe { self.columns[buffer_index].1.view_mut(*self.num_rows) }
    }

    /// Set number of valid rows in the buffer. May not be larger than the batch size. If the
    /// specified number should be larger than the number of valid rows currently held by the buffer
    /// additional rows with the default value are going to be created.
    pub fn set_num_rows(&mut self, num_rows: usize) {
        if num_rows > self.max_rows as usize {
            panic!(
                "Columnar buffer may not be resized to a value higher than the maximum number of \
                rows initially specified in the constructor."
            );
        }
        if *self.num_rows < num_rows {
            for (_col_index, ref mut column) in &mut self.columns {
                column.fill_default(*self.num_rows, num_rows)
            }
        }
        *self.num_rows = num_rows;
    }
}

unsafe impl<C> RowSetBuffer for ColumnarRowSet<C>
where
    C: ColumnBuffer,
{
    fn bind_type(&self) -> usize {
        0 // Specify columnar binding
    }

    fn row_array_size(&self) -> usize {
        self.max_rows
    }

    fn mut_num_fetch_rows(&mut self) -> &mut usize {
        self.num_rows.as_mut()
    }

    unsafe fn bind_to_cursor(&mut self, cursor: &mut impl Cursor) -> Result<(), Error> {
        for (col_number, column) in &mut self.columns {
            cursor
                .stmt_mut()
                .bind_col(*col_number, column)
                .into_result(cursor.stmt_mut())?;
        }
        Ok(())
    }
}

unsafe impl<C> ParameterRefCollection for &ColumnarRowSet<C>
where
    C: ColumnBuffer,
{
    fn parameter_set_size(&self) -> usize {
        *self.num_rows
    }

    unsafe fn bind_parameters_to(&mut self, stmt: &mut impl Statement) -> Result<(), Error> {
        for &(parameter_number, ref buffer) in &self.columns {
            stmt.bind_input_parameter(parameter_number, buffer)
                .into_result(stmt)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::{AnyColumnBuffer, BufferDescription, BufferKind, ColumnarRowSet};

    #[test]
    #[should_panic(expected = "Column indices must be unique.")]
    fn assert_unique_column_indices() {
        let bd = BufferDescription {
            nullable: false,
            kind: BufferKind::I32,
        };
        ColumnarRowSet::<AnyColumnBuffer>::with_column_indices(
            1,
            [(1, bd), (2, bd), (1, bd)].iter().cloned(),
        );
    }
}

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
    columnar::ColumnBuffer,
    text_column::TextColumnSliceMut,
    BinColumn, BinColumnView, BufferDesc, CharColumn, ColumnarBuffer, Indicator, Item,
    NullableSlice, NullableSliceMut, TextColumn, TextColumnView, WCharColumn,
};

/// Since buffer shapes are same for all time / timestamps independent of the precision and we do
/// not know the precise SQL type. In order to still be able to bind time / timestamp buffer as
/// input without requiring the user to separately specify the precision, we declare 100 Nano second
/// precision. This was the highest precision still supported by MSSQL in the tests.
const DEFAULT_TIME_PRECISION: i16 = 7;

/// Buffer holding a single column of either a result set or paramater
#[derive(Debug)]
pub enum AnyBuffer {
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

impl AnyBuffer {
    /// Map buffer description to actual buffer.
    pub fn try_from_desc(max_rows: usize, desc: BufferDesc) -> Result<Self, TooLargeBufferSize> {
        let fallible_allocations = true;
        Self::impl_from_desc(max_rows, desc, fallible_allocations)
    }

    /// Map buffer description to actual buffer.
    pub fn from_desc(max_rows: usize, desc: BufferDesc) -> Self {
        let fallible_allocations = false;
        Self::impl_from_desc(max_rows, desc, fallible_allocations).unwrap()
    }

    /// Map buffer description to actual buffer.
    fn impl_from_desc(
        max_rows: usize,
        desc: BufferDesc,
        fallible_allocations: bool,
    ) -> Result<Self, TooLargeBufferSize> {
        let buffer = match desc {
            BufferDesc::Binary { length } => {
                if fallible_allocations {
                    AnyBuffer::Binary(BinColumn::try_new(max_rows, length)?)
                } else {
                    AnyBuffer::Binary(BinColumn::new(max_rows, length))
                }
            }
            BufferDesc::Text { max_str_len } => {
                if fallible_allocations {
                    AnyBuffer::Text(TextColumn::try_new(max_rows, max_str_len)?)
                } else {
                    AnyBuffer::Text(TextColumn::new(max_rows, max_str_len))
                }
            }
            BufferDesc::WText { max_str_len } => {
                if fallible_allocations {
                    AnyBuffer::WText(TextColumn::try_new(max_rows, max_str_len)?)
                } else {
                    AnyBuffer::WText(TextColumn::new(max_rows, max_str_len))
                }
            }
            BufferDesc::Date { nullable: false } => {
                AnyBuffer::Date(vec![Date::default(); max_rows])
            }
            BufferDesc::Time { nullable: false } => {
                AnyBuffer::Time(vec![Time::default(); max_rows])
            }
            BufferDesc::Timestamp { nullable: false } => {
                AnyBuffer::Timestamp(vec![Timestamp::default(); max_rows])
            }
            BufferDesc::F64 { nullable: false } => AnyBuffer::F64(vec![f64::default(); max_rows]),
            BufferDesc::F32 { nullable: false } => AnyBuffer::F32(vec![f32::default(); max_rows]),
            BufferDesc::I8 { nullable: false } => AnyBuffer::I8(vec![i8::default(); max_rows]),
            BufferDesc::I16 { nullable: false } => AnyBuffer::I16(vec![i16::default(); max_rows]),
            BufferDesc::I32 { nullable: false } => AnyBuffer::I32(vec![i32::default(); max_rows]),
            BufferDesc::I64 { nullable: false } => AnyBuffer::I64(vec![i64::default(); max_rows]),
            BufferDesc::U8 { nullable: false } => AnyBuffer::U8(vec![u8::default(); max_rows]),
            BufferDesc::Bit { nullable: false } => AnyBuffer::Bit(vec![Bit::default(); max_rows]),
            BufferDesc::Date { nullable: true } => {
                AnyBuffer::NullableDate(OptDateColumn::new(max_rows))
            }
            BufferDesc::Time { nullable: true } => {
                AnyBuffer::NullableTime(OptTimeColumn::new(max_rows))
            }
            BufferDesc::Timestamp { nullable: true } => {
                AnyBuffer::NullableTimestamp(OptTimestampColumn::new(max_rows))
            }
            BufferDesc::F64 { nullable: true } => {
                AnyBuffer::NullableF64(OptF64Column::new(max_rows))
            }
            BufferDesc::F32 { nullable: true } => {
                AnyBuffer::NullableF32(OptF32Column::new(max_rows))
            }
            BufferDesc::I8 { nullable: true } => AnyBuffer::NullableI8(OptI8Column::new(max_rows)),
            BufferDesc::I16 { nullable: true } => {
                AnyBuffer::NullableI16(OptI16Column::new(max_rows))
            }
            BufferDesc::I32 { nullable: true } => {
                AnyBuffer::NullableI32(OptI32Column::new(max_rows))
            }
            BufferDesc::I64 { nullable: true } => {
                AnyBuffer::NullableI64(OptI64Column::new(max_rows))
            }
            BufferDesc::U8 { nullable: true } => AnyBuffer::NullableU8(OptU8Column::new(max_rows)),
            BufferDesc::Bit { nullable: true } => {
                AnyBuffer::NullableBit(OptBitColumn::new(max_rows))
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
            AnyBuffer::Binary(col) => col,
            AnyBuffer::Text(col) => col,
            AnyBuffer::WText(col) => col,
            AnyBuffer::F64(col) => col,
            AnyBuffer::F32(col) => col,
            AnyBuffer::Date(col) => col,
            AnyBuffer::Time(col) => col,
            AnyBuffer::Timestamp(col) => col,
            AnyBuffer::I8(col) => col,
            AnyBuffer::I16(col) => col,
            AnyBuffer::I32(col) => col,
            AnyBuffer::I64(col) => col,
            AnyBuffer::Bit(col) => col,
            AnyBuffer::U8(col) => col,
            AnyBuffer::NullableF64(col) => col,
            AnyBuffer::NullableF32(col) => col,
            AnyBuffer::NullableDate(col) => col,
            AnyBuffer::NullableTime(col) => col,
            AnyBuffer::NullableTimestamp(col) => col,
            AnyBuffer::NullableI8(col) => col,
            AnyBuffer::NullableI16(col) => col,
            AnyBuffer::NullableI32(col) => col,
            AnyBuffer::NullableI64(col) => col,
            AnyBuffer::NullableBit(col) => col,
            AnyBuffer::NullableU8(col) => col,
        }
    }

    fn inner_cdata_mut(&mut self) -> &mut dyn CDataMut {
        match self {
            AnyBuffer::Binary(col) => col,
            AnyBuffer::Text(col) => col,
            AnyBuffer::WText(col) => col,
            AnyBuffer::F64(col) => col,
            AnyBuffer::F32(col) => col,
            AnyBuffer::Date(col) => col,
            AnyBuffer::Time(col) => col,
            AnyBuffer::Timestamp(col) => col,
            AnyBuffer::I8(col) => col,
            AnyBuffer::I16(col) => col,
            AnyBuffer::I32(col) => col,
            AnyBuffer::I64(col) => col,
            AnyBuffer::Bit(col) => col,
            AnyBuffer::U8(col) => col,
            AnyBuffer::NullableF64(col) => col,
            AnyBuffer::NullableF32(col) => col,
            AnyBuffer::NullableDate(col) => col,
            AnyBuffer::NullableTime(col) => col,
            AnyBuffer::NullableTimestamp(col) => col,
            AnyBuffer::NullableI8(col) => col,
            AnyBuffer::NullableI16(col) => col,
            AnyBuffer::NullableI32(col) => col,
            AnyBuffer::NullableI64(col) => col,
            AnyBuffer::NullableBit(col) => col,
            AnyBuffer::NullableU8(col) => col,
        }
    }
}

unsafe impl CData for AnyBuffer {
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

unsafe impl CDataMut for AnyBuffer {
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        self.inner_cdata_mut().mut_indicator_ptr()
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.inner_cdata_mut().mut_value_ptr()
    }
}

impl HasDataType for AnyBuffer {
    fn data_type(&self) -> DataType {
        match self {
            AnyBuffer::Binary(col) => col.data_type(),
            AnyBuffer::Text(col) => col.data_type(),
            AnyBuffer::WText(col) => col.data_type(),
            AnyBuffer::Date(_) | AnyBuffer::NullableDate(_) => DataType::Date,
            AnyBuffer::Time(_) | AnyBuffer::NullableTime(_) => DataType::Time {
                precision: DEFAULT_TIME_PRECISION,
            },
            AnyBuffer::Timestamp(_) | AnyBuffer::NullableTimestamp(_) => DataType::Timestamp {
                precision: DEFAULT_TIME_PRECISION,
            },
            AnyBuffer::F64(_) | AnyBuffer::NullableF64(_) => DataType::Double,
            AnyBuffer::F32(_) | AnyBuffer::NullableF32(_) => DataType::Real,
            AnyBuffer::I8(_) | AnyBuffer::NullableI8(_) => DataType::TinyInt,
            AnyBuffer::I16(_) | AnyBuffer::NullableI16(_) => DataType::SmallInt,
            AnyBuffer::I32(_) | AnyBuffer::NullableI32(_) => DataType::Integer,
            AnyBuffer::I64(_) | AnyBuffer::NullableI64(_) => DataType::BigInt,
            // Few databases support unsigned types, binding U8 as tiny int might lead to weird
            // stuff if the database has type is signed. I guess. Let's bind it as SmallInt by
            // default, just to be on the safe side.
            AnyBuffer::U8(_) | AnyBuffer::NullableU8(_) => DataType::SmallInt,
            AnyBuffer::Bit(_) | AnyBuffer::NullableBit(_) => DataType::Bit,
        }
    }
}

/// Flexible columnar buffer implementation. Bind this to a cursor to fetch values in bulk, or pass
/// this as a parameter to a statement, to submit many parameters at once.
pub type ColumnarAnyBuffer = ColumnarBuffer<AnyBuffer>;

impl ColumnarAnyBuffer {
    /// Allocates a [`ColumnarBuffer`] fitting the buffer descriptions.
    pub fn from_descs(capacity: usize, descs: impl IntoIterator<Item = BufferDesc>) -> Self {
        let mut column_index = 0;
        let columns = descs
            .into_iter()
            .map(move |desc| {
                let buffer = AnyBuffer::from_desc(capacity, desc);
                column_index += 1;
                (column_index, buffer)
            })
            .collect();
        unsafe { ColumnarBuffer::new_unchecked(capacity, columns) }
    }

    /// Allocates a [`ColumnarBuffer`] fitting the buffer descriptions. If not enough memory is
    /// available to allocate the buffers this function fails with
    /// [`Error::TooLargeColumnBufferSize`]. This function is slower than [`Self::from_descs`]
    /// which would just panic if not enough memory is available for allocation.
    pub fn try_from_descs(
        capacity: usize,
        descs: impl IntoIterator<Item = BufferDesc>,
    ) -> Result<Self, Error> {
        let mut column_index = 0;
        let columns = descs
            .into_iter()
            .map(move |desc| {
                let buffer = AnyBuffer::try_from_desc(capacity, desc)
                    .map_err(|source| source.add_context(column_index))?;
                column_index += 1;
                Ok::<_, Error>((column_index, buffer))
            })
            .collect::<Result<_, _>>()?;
        Ok(unsafe { ColumnarBuffer::new_unchecked(capacity, columns) })
    }

    /// Allows you to pass the buffer descriptions together with a one based column index referring
    /// the column, the buffer is supposed to bind to. This allows you also to ignore columns in a
    /// result set, by not binding them at all. There is no restriction on the order of column
    /// indices passed, but the function will panic, if the indices are not unique.
    pub fn from_descs_and_indices(
        max_rows: usize,
        description: impl Iterator<Item = (u16, BufferDesc)>,
    ) -> ColumnarBuffer<AnyBuffer> {
        let columns: Vec<_> = description
            .map(|(col_index, buffer_desc)| {
                (col_index, AnyBuffer::from_desc(max_rows, buffer_desc))
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
pub enum AnySlice<'a> {
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

impl<'a> AnySlice<'a> {
    /// This method is useful if you expect the variant to be [`AnySlice::Text`]. It allows you to
    /// unwrap the inner column view without explictly matching it.
    pub fn as_text_view(self) -> Option<TextColumnView<'a, u8>> {
        if let Self::Text(view) = self {
            Some(view)
        } else {
            None
        }
    }

    /// This method is useful if you expect the variant to be [`AnySlice::WText`]. It allows you to
    /// unwrap the inner column view without explictly matching it.
    pub fn as_w_text_view(self) -> Option<TextColumnView<'a, u16>> {
        if let Self::WText(view) = self {
            Some(view)
        } else {
            None
        }
    }

    /// This method is useful if you expect the variant to be [`AnySlice::Binary`]. It allows you to
    /// unwrap the inner column view without explictly matching it.
    pub fn as_bin_view(self) -> Option<BinColumnView<'a>> {
        if let Self::Binary(view) = self {
            Some(view)
        } else {
            None
        }
    }

    /// Extract the array type from an [`AnySlice`].
    pub fn as_slice<I: Item>(self) -> Option<&'a [I]> {
        I::as_slice(self)
    }

    /// Extract the typed nullable buffer from an [`AnySlice`].
    pub fn as_nullable_slice<I: Item>(self) -> Option<NullableSlice<'a, I>> {
        I::as_nullable_slice(self)
    }
}

unsafe impl<'a> BoundInputSlice<'a> for AnyBuffer {
    type SliceMut = AnySliceMut<'a>;

    unsafe fn as_view_mut(
        &'a mut self,
        parameter_index: u16,
        stmt: StatementRef<'a>,
    ) -> Self::SliceMut {
        let num_rows = self.capacity();
        match self {
            AnyBuffer::Binary(column) => {
                AnySliceMut::Binary(column.as_view_mut(parameter_index, stmt))
            }
            AnyBuffer::Text(column) => AnySliceMut::Text(column.as_view_mut(parameter_index, stmt)),
            AnyBuffer::WText(column) => {
                AnySliceMut::WText(column.as_view_mut(parameter_index, stmt))
            }
            AnyBuffer::Date(column) => AnySliceMut::Date(column),
            AnyBuffer::Time(column) => AnySliceMut::Time(column),
            AnyBuffer::Timestamp(column) => AnySliceMut::Timestamp(column),
            AnyBuffer::F64(column) => AnySliceMut::F64(column),
            AnyBuffer::F32(column) => AnySliceMut::F32(column),
            AnyBuffer::I8(column) => AnySliceMut::I8(column),
            AnyBuffer::I16(column) => AnySliceMut::I16(column),
            AnyBuffer::I32(column) => AnySliceMut::I32(column),
            AnyBuffer::I64(column) => AnySliceMut::I64(column),
            AnyBuffer::U8(column) => AnySliceMut::U8(column),
            AnyBuffer::Bit(column) => AnySliceMut::Bit(column),
            AnyBuffer::NullableDate(column) => AnySliceMut::NullableDate(column.writer_n(num_rows)),
            AnyBuffer::NullableTime(column) => AnySliceMut::NullableTime(column.writer_n(num_rows)),
            AnyBuffer::NullableTimestamp(column) => {
                AnySliceMut::NullableTimestamp(column.writer_n(num_rows))
            }
            AnyBuffer::NullableF64(column) => AnySliceMut::NullableF64(column.writer_n(num_rows)),
            AnyBuffer::NullableF32(column) => AnySliceMut::NullableF32(column.writer_n(num_rows)),
            AnyBuffer::NullableI8(column) => AnySliceMut::NullableI8(column.writer_n(num_rows)),
            AnyBuffer::NullableI16(column) => AnySliceMut::NullableI16(column.writer_n(num_rows)),
            AnyBuffer::NullableI32(column) => AnySliceMut::NullableI32(column.writer_n(num_rows)),
            AnyBuffer::NullableI64(column) => AnySliceMut::NullableI64(column.writer_n(num_rows)),
            AnyBuffer::NullableU8(column) => AnySliceMut::NullableU8(column.writer_n(num_rows)),
            AnyBuffer::NullableBit(column) => AnySliceMut::NullableBit(column.writer_n(num_rows)),
        }
    }
}

/// A mutable slice of an input buffer, with runtime type information. Edit values in this slice in
/// order to send parameters in bulk to a database.
pub enum AnySliceMut<'a> {
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

impl<'a> AnySliceMut<'a> {
    /// This method is useful if you expect the variant to be [`AnySliceMut::Binary`]. It allows you
    /// to unwrap the inner column view without explictly matching it.
    pub fn as_bin_view(self) -> Option<BinColumnSliceMut<'a>> {
        if let Self::Binary(view) = self {
            Some(view)
        } else {
            None
        }
    }

    /// This method is useful if you expect the variant to be [`AnySliceMut::Text`]. It allows you
    /// to unwrap the inner column view without explictly matching it.
    pub fn as_text_view(self) -> Option<TextColumnSliceMut<'a, u8>> {
        if let Self::Text(view) = self {
            Some(view)
        } else {
            None
        }
    }

    /// This method is useful if you expect the variant to be [`AnySliceMut::WText`]. It allows you
    /// to unwrap the inner column view without explictly matching it.
    pub fn as_w_text_view(self) -> Option<TextColumnSliceMut<'a, u16>> {
        if let Self::WText(view) = self {
            Some(view)
        } else {
            None
        }
    }

    /// Extract the array type from an [`AnySliceMut`].
    pub fn as_slice<I: Item>(self) -> Option<&'a mut [I]> {
        I::as_slice_mut(self)
    }

    /// Extract the typed nullable buffer from an [`AnySliceMut`].
    pub fn as_nullable_slice<I: Item>(self) -> Option<NullableSliceMut<'a, I>> {
        I::as_nullable_slice_mut(self)
    }
}

unsafe impl ColumnBuffer for AnyBuffer {
    type View<'a> = AnySlice<'a>;

    fn capacity(&self) -> usize {
        match self {
            AnyBuffer::Binary(col) => col.capacity(),
            AnyBuffer::Text(col) => col.capacity(),
            AnyBuffer::WText(col) => col.capacity(),
            AnyBuffer::Date(col) => col.capacity(),
            AnyBuffer::Time(col) => col.capacity(),
            AnyBuffer::Timestamp(col) => col.capacity(),
            AnyBuffer::F64(col) => col.capacity(),
            AnyBuffer::F32(col) => col.capacity(),
            AnyBuffer::I8(col) => col.capacity(),
            AnyBuffer::I16(col) => col.capacity(),
            AnyBuffer::I32(col) => col.capacity(),
            AnyBuffer::I64(col) => col.capacity(),
            AnyBuffer::U8(col) => col.capacity(),
            AnyBuffer::Bit(col) => col.capacity(),
            AnyBuffer::NullableDate(col) => col.capacity(),
            AnyBuffer::NullableTime(col) => col.capacity(),
            AnyBuffer::NullableTimestamp(col) => col.capacity(),
            AnyBuffer::NullableF64(col) => col.capacity(),
            AnyBuffer::NullableF32(col) => col.capacity(),
            AnyBuffer::NullableI8(col) => col.capacity(),
            AnyBuffer::NullableI16(col) => col.capacity(),
            AnyBuffer::NullableI32(col) => col.capacity(),
            AnyBuffer::NullableI64(col) => col.capacity(),
            AnyBuffer::NullableU8(col) => col.capacity(),
            AnyBuffer::NullableBit(col) => col.capacity(),
        }
    }

    fn view(&self, valid_rows: usize) -> AnySlice {
        match self {
            AnyBuffer::Binary(col) => AnySlice::Binary(col.view(valid_rows)),
            AnyBuffer::Text(col) => AnySlice::Text(col.view(valid_rows)),
            AnyBuffer::WText(col) => AnySlice::WText(col.view(valid_rows)),
            AnyBuffer::Date(col) => AnySlice::Date(&col[0..valid_rows]),
            AnyBuffer::Time(col) => AnySlice::Time(&col[0..valid_rows]),
            AnyBuffer::Timestamp(col) => AnySlice::Timestamp(&col[0..valid_rows]),
            AnyBuffer::F64(col) => AnySlice::F64(&col[0..valid_rows]),
            AnyBuffer::F32(col) => AnySlice::F32(&col[0..valid_rows]),
            AnyBuffer::I8(col) => AnySlice::I8(&col[0..valid_rows]),
            AnyBuffer::I16(col) => AnySlice::I16(&col[0..valid_rows]),
            AnyBuffer::I32(col) => AnySlice::I32(&col[0..valid_rows]),
            AnyBuffer::I64(col) => AnySlice::I64(&col[0..valid_rows]),
            AnyBuffer::U8(col) => AnySlice::U8(&col[0..valid_rows]),
            AnyBuffer::Bit(col) => AnySlice::Bit(&col[0..valid_rows]),
            AnyBuffer::NullableDate(col) => AnySlice::NullableDate(col.iter(valid_rows)),
            AnyBuffer::NullableTime(col) => AnySlice::NullableTime(col.iter(valid_rows)),
            AnyBuffer::NullableTimestamp(col) => AnySlice::NullableTimestamp(col.iter(valid_rows)),
            AnyBuffer::NullableF64(col) => AnySlice::NullableF64(col.iter(valid_rows)),
            AnyBuffer::NullableF32(col) => AnySlice::NullableF32(col.iter(valid_rows)),
            AnyBuffer::NullableI8(col) => AnySlice::NullableI8(col.iter(valid_rows)),
            AnyBuffer::NullableI16(col) => AnySlice::NullableI16(col.iter(valid_rows)),
            AnyBuffer::NullableI32(col) => AnySlice::NullableI32(col.iter(valid_rows)),
            AnyBuffer::NullableI64(col) => AnySlice::NullableI64(col.iter(valid_rows)),
            AnyBuffer::NullableU8(col) => AnySlice::NullableU8(col.iter(valid_rows)),
            AnyBuffer::NullableBit(col) => AnySlice::NullableBit(col.iter(valid_rows)),
        }
    }

    /// Fills the column with the default representation of values, between `from` and `to` index.
    fn fill_default(&mut self, from: usize, to: usize) {
        match self {
            AnyBuffer::Binary(col) => col.fill_null(from, to),
            AnyBuffer::Text(col) => col.fill_null(from, to),
            AnyBuffer::WText(col) => col.fill_null(from, to),
            AnyBuffer::Date(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyBuffer::Time(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyBuffer::Timestamp(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyBuffer::F64(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyBuffer::F32(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyBuffer::I8(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyBuffer::I16(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyBuffer::I32(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyBuffer::I64(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyBuffer::U8(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyBuffer::Bit(col) => Self::fill_default_slice(&mut col[from..to]),
            AnyBuffer::NullableDate(col) => col.fill_null(from, to),
            AnyBuffer::NullableTime(col) => col.fill_null(from, to),
            AnyBuffer::NullableTimestamp(col) => col.fill_null(from, to),
            AnyBuffer::NullableF64(col) => col.fill_null(from, to),
            AnyBuffer::NullableF32(col) => col.fill_null(from, to),
            AnyBuffer::NullableI8(col) => col.fill_null(from, to),
            AnyBuffer::NullableI16(col) => col.fill_null(from, to),
            AnyBuffer::NullableI32(col) => col.fill_null(from, to),
            AnyBuffer::NullableI64(col) => col.fill_null(from, to),
            AnyBuffer::NullableU8(col) => col.fill_null(from, to),
            AnyBuffer::NullableBit(col) => col.fill_null(from, to),
        }
    }

    fn has_truncated_values(&self, num_rows: usize) -> Option<Indicator> {
        match self {
            AnyBuffer::Binary(col) => col.has_truncated_values(num_rows),
            AnyBuffer::Text(col) => col.has_truncated_values(num_rows),
            AnyBuffer::WText(col) => col.has_truncated_values(num_rows),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffers::{AnySlice, AnySliceMut, ColumnBuffer};

    use super::AnyBuffer;

    #[test]
    fn slice_should_only_contain_part_of_the_buffer() {
        let buffer = AnyBuffer::I32(vec![1, 2, 3]);

        let view = buffer.view(2);

        assert_eq!(Some([1, 2].as_slice()), view.as_slice::<i32>());
    }

    #[test]
    fn slice_should_be_none_if_types_mismatch() {
        let buffer = [1, 2, 3];
        let view = AnySlice::I32(&buffer);
        assert_eq!(None, view.as_slice::<i16>());
    }

    #[test]
    fn slice_mut_should_be_none_if_types_mismatch() {
        let mut buffer = [1, 2, 3];
        let view = AnySliceMut::I32(&mut buffer);
        assert_eq!(None, view.as_slice::<i16>());
    }

    #[test]
    fn nullable_slice_should_be_none_if_buffer_is_non_nullable() {
        let buffer = [1, 2, 3];
        let view = AnySlice::I32(&buffer);
        assert!(view.as_nullable_slice::<i32>().is_none());
    }

    #[test]
    fn nullable_slice_mut_should_be_none_if_buffer_is_non_nullable() {
        let mut buffer = [1, 2, 3];
        let view = AnySliceMut::I32(&mut buffer);
        assert!(view.as_nullable_slice::<i32>().is_none());
    }
}

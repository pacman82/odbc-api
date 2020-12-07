use std::{collections::HashSet, ffi::c_void};

use crate::{
    fixed_sized::Bit,
    handles::{CData, CDataMut},
    Error, RowSetBuffer,
};

use super::{
    column_with_indicator::{
        OptBitColumn, OptDateColumn, OptF32Column, OptF64Column, OptI16Column, OptI32Column,
        OptI64Column, OptI8Column, OptIt, OptTimeColumn, OptTimestampColumn, OptU8Column,
    },
    BufferDescription, BufferKind, TextColumn, TextColumnIt,
};

use odbc_sys::{CDataType, Date, Time, Timestamp};

/// A borrowed view on the valid rows in a column of a [`crate::buffers::ColumnarRowSet`].
///
/// For columns of fixed size types, which are guaranteed to not contain null, a direct access to
/// the slice is offered. Buffers over nullable columns can be accessed via an iterator over
/// options.
#[derive(Debug)]
pub enum AnyColumnView<'a> {
    Text(TextColumnIt<'a>),
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
    NullableDate(OptIt<'a, Date>),
    NullableTime(OptIt<'a, Time>),
    NullableTimestamp(OptIt<'a, Timestamp>),
    NullableF64(OptIt<'a, f64>),
    NullableF32(OptIt<'a, f32>),
    NullableI8(OptIt<'a, i8>),
    NullableI16(OptIt<'a, i16>),
    NullableI32(OptIt<'a, i32>),
    NullableI64(OptIt<'a, i64>),
    NullableU8(OptIt<'a, u8>),
    NullableBit(OptIt<'a, Bit>),
}

#[derive(Debug)]
enum AnyColumnBuffer {
    Text(TextColumn),
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
    pub fn new(max_rows: u32, desc: BufferDescription) -> Self {
        match (desc.kind, desc.nullable) {
            (BufferKind::Text { max_str_len }, _) => {
                AnyColumnBuffer::Text(TextColumn::new(max_rows as usize, max_str_len))
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

    fn inner_cdata(&self) -> &dyn CData {
        match self {
            AnyColumnBuffer::Text(col) => col,
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
            AnyColumnBuffer::Text(col) => col,
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

    pub unsafe fn view(&self, num_rows: usize) -> AnyColumnView {
        match self {
            AnyColumnBuffer::Text(col) => AnyColumnView::Text(col.iter(num_rows)),
            AnyColumnBuffer::Date(col) => AnyColumnView::Date(&col[0..num_rows]),
            AnyColumnBuffer::Time(col) => AnyColumnView::Time(&col[0..num_rows]),
            AnyColumnBuffer::Timestamp(col) => AnyColumnView::Timestamp(&col[0..num_rows]),
            AnyColumnBuffer::F64(col) => AnyColumnView::F64(&col[0..num_rows]),
            AnyColumnBuffer::F32(col) => AnyColumnView::F32(&col[0..num_rows]),
            AnyColumnBuffer::I8(col) => AnyColumnView::I8(&col[0..num_rows]),
            AnyColumnBuffer::I16(col) => AnyColumnView::I16(&col[0..num_rows]),
            AnyColumnBuffer::I32(col) => AnyColumnView::I32(&col[0..num_rows]),
            AnyColumnBuffer::I64(col) => AnyColumnView::I64(&col[0..num_rows]),
            AnyColumnBuffer::U8(col) => AnyColumnView::U8(&col[0..num_rows]),
            AnyColumnBuffer::Bit(col) => AnyColumnView::Bit(&col[0..num_rows]),
            AnyColumnBuffer::NullableDate(col) => AnyColumnView::NullableDate(col.iter(num_rows)),
            AnyColumnBuffer::NullableTime(col) => AnyColumnView::NullableTime(col.iter(num_rows)),
            AnyColumnBuffer::NullableTimestamp(col) => {
                AnyColumnView::NullableTimestamp(col.iter(num_rows))
            }
            AnyColumnBuffer::NullableF64(col) => AnyColumnView::NullableF64(col.iter(num_rows)),
            AnyColumnBuffer::NullableF32(col) => AnyColumnView::NullableF32(col.iter(num_rows)),
            AnyColumnBuffer::NullableI8(col) => AnyColumnView::NullableI8(col.iter(num_rows)),
            AnyColumnBuffer::NullableI16(col) => AnyColumnView::NullableI16(col.iter(num_rows)),
            AnyColumnBuffer::NullableI32(col) => AnyColumnView::NullableI32(col.iter(num_rows)),
            AnyColumnBuffer::NullableI64(col) => AnyColumnView::NullableI64(col.iter(num_rows)),
            AnyColumnBuffer::NullableU8(col) => AnyColumnView::NullableU8(col.iter(num_rows)),
            AnyColumnBuffer::NullableBit(col) => AnyColumnView::NullableBit(col.iter(num_rows)),
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

/// A columnar buffer intended to be bound with [crate::Cursor::bind_buffer] in order to obtain
/// results from a cursor.
///
/// This buffer is designed to be versatile. It supports a wide variaty of usage scenarios. It is
/// efficient in retrieving data, but expensive to allocate, as columns are allocated seperatly.
/// This is required in order to efficiently allow for rebinding columns, if this buffer is used to
/// provide array input parameters those maximum size is not known in advance.
///
/// Most applications should find the overhead neglegible, especially if instances are reused.
pub struct ColumnarRowSet {
    /// Use a box, so it is safe for a cursor to take ownership of this buffer.
    num_rows: Box<usize>,
    /// aka: batch size, row array size
    max_rows: u32,
    /// Column index and bound buffer
    columns: Vec<(u16, AnyColumnBuffer)>,
}

impl ColumnarRowSet {
    /// Allocates for each buffer description a buffer large enough to hold `max_rows`.
    pub fn new(max_rows: u32, description: impl Iterator<Item = BufferDescription>) -> Self {
        let mut column_index = 0;
        let columns = description
            .map(move |desc| {
                column_index += 1;
                (column_index, AnyColumnBuffer::new(max_rows, desc))
            })
            .collect();
        ColumnarRowSet {
            num_rows: Box::new(0),
            max_rows,
            columns,
        }
    }

    /// Allows you to pass the buffer descriptions together with a one based column index refering
    /// the column, the buffer is supposed to bind to. This allows you also to ignore columns in a
    /// result set, by not binding them at all. There is no restriction on the order of column
    /// indices passed, but the function will panic, if the indices are not unique.
    pub fn with_column_indices(
        max_rows: u32,
        description: impl Iterator<Item = (u16, BufferDescription)>,
    ) -> Self {
        let columns: Vec<_> = description
            .map(|(col_index, buffer_desc)| {
                (col_index, AnyColumnBuffer::new(max_rows, buffer_desc))
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

        ColumnarRowSet {
            num_rows: Box::new(0),
            max_rows,
            columns,
        }
    }

    /// Use this method to gain access to the actual column data.
    ///
    /// # Prameters
    ///
    /// * `buffer_index`: Please note that the buffer index is not indentical to the ODBC column
    ///   index. For once it is zero based. It also indexes the buffer bound, and not the columns of
    ///   the output result set. This is important, because not every column needs to be bound. Some
    ///   columns may simply be ignored. That being said, if every column of the output should is
    ///   bound in the buffer, in the same order in which they are enumerated in the result set, the
    ///   relationship betwenn column index and buffer index is `buffer_index = column_index - 1'.
    pub fn column(&self, buffer_index: usize) -> AnyColumnView<'_> {
        unsafe { self.columns[buffer_index].1.view(*self.num_rows) }
    }

    /// Number of valid rows in the buffer.
    pub fn num_rows(&self) -> usize {
        *self.num_rows
    }
}

unsafe impl RowSetBuffer for ColumnarRowSet {
    fn bind_type(&self) -> u32 {
        0 // Specify columnar binding
    }

    fn row_array_size(&self) -> u32 {
        self.max_rows
    }

    fn mut_num_fetch_rows(&mut self) -> &mut usize {
        self.num_rows.as_mut()
    }

    unsafe fn bind_to_cursor(&mut self, cursor: &mut impl crate::Cursor) -> Result<(), Error> {
        for (col_number, column) in &mut self.columns {
            cursor.bind_col(*col_number, column)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::{BufferDescription, BufferKind, ColumnarRowSet};

    #[test]
    #[should_panic(expected = "Column indices must be unique.")]
    fn assert_unique_column_indices() {
        let bd = BufferDescription {
            nullable: false,
            kind: BufferKind::I32,
        };
        ColumnarRowSet::with_column_indices(1, [(1, bd), (2, bd), (1, bd)].iter().cloned());
    }
}

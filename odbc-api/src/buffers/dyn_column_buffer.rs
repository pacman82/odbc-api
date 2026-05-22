use super::{
    BinColumn, BinColumnSlice, BinColumnSliceMut, BufferDesc, ColumnBuffer, ColumnarBuffer,
    Indicator, NullableSlice, NullableSliceMut, Resize, Slice, TextColumn, TextColumnSlice,
    TextColumnSliceMut, column_with_indicator::ColumnWithIndicator,
};

use crate::{
    BoundInputSlice, Error,
    fixed_sized::Pod,
    handles::{CData, CDataMut, StatementRef},
};

use std::{any::Any, collections::HashSet, ffi::c_void};

/// Columnar buffer with dynamic column types decided at runtime.
///
/// Go to buffer implementation for this crate if fetching data. If you have no reason to use
/// something else use this.
pub type ColumnarDynBuffer = ColumnarBuffer<BoxColumnBuffer>;

impl ColumnarDynBuffer {
    /// Allocates a [`ColumnarDynBuffer`] fitting the buffer descriptions.
    pub fn from_descs(capacity: usize, descs: impl IntoIterator<Item = BufferDesc>) -> Self {
        let mut column_index = 0;
        let columns = descs
            .into_iter()
            .map(move |desc| {
                let buffer = desc.column_buffer(capacity);
                column_index += 1;
                (column_index, buffer)
            })
            .collect();
        unsafe { ColumnarBuffer::new_unchecked(capacity, columns) }
    }

    /// Allocates a [`ColumnarDynBuffer`] fitting the buffer descriptions. If not enough memory is
    /// available to allocate the buffers this function fails with
    /// [`Error::TooLargeColumnBufferSize`]. This function is slower than [`Self::from_descs`] which
    /// would just panic if not enough memory is available for allocation.
    pub fn try_from_descs(
        capacity: usize,
        descs: impl IntoIterator<Item = BufferDesc>,
    ) -> Result<Self, Error> {
        let mut column_index = 0;
        let columns = descs
            .into_iter()
            .map(move |desc| {
                let buffer = desc
                    .try_column_buffer(capacity)
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
    ) -> Self {
        let columns: Vec<_> = description
            .map(|(col_index, buffer_desc)| (col_index, buffer_desc.column_buffer(max_rows)))
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

/// Heap allocated column buffer with dynamic type. Intended to be used as a type parameter for
/// [`ColumnarBuffer`] to enable columns of different types only known at runtime to be bound to the
/// same block cursor.
pub type BoxColumnBuffer = Box<dyn AnyColumnBuffer>;

/// A [`ColumnBuffer`] that additionally carries runtime type information, so a trait object can
/// be downcast to the concrete buffer type it was constructed from. Intended for [`ColumnarBuffer`]
/// with heterogenous columns, i.e. integer and text columns.
///
/// This trait also inherits from`Send` so we can use it in concurrent bulk fetches using double
/// buffering in multiple threads. Currently there are no buffer implementations which are not
/// `Send`. So a distinction between `Send` and non-`Send` buffers is assumed to cause more friction
/// rather than being helpful.
pub trait AnyColumnBuffer: ColumnBuffer + Resize + Any + Send {}

impl<T> AnyColumnBuffer for T where T: ColumnBuffer + Resize + Any + Send {}

unsafe impl CData for BoxColumnBuffer {
    fn cdata_type(&self) -> odbc_sys::CDataType {
        self.as_ref().cdata_type()
    }

    fn indicator_ptr(&self) -> *const isize {
        self.as_ref().indicator_ptr()
    }

    fn value_ptr(&self) -> *const c_void {
        self.as_ref().value_ptr()
    }

    fn buffer_length(&self) -> isize {
        self.as_ref().buffer_length()
    }
}

unsafe impl CDataMut for BoxColumnBuffer {
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        self.as_mut().mut_indicator_ptr()
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.as_mut().mut_value_ptr()
    }
}

unsafe impl ColumnBuffer for BoxColumnBuffer {
    fn capacity(&self) -> usize {
        self.as_ref().capacity()
    }

    fn has_truncated_values(&self, num_rows: usize) -> Option<Indicator> {
        self.as_ref().has_truncated_values(num_rows)
    }
}

unsafe impl Slice for BoxColumnBuffer {
    type Slice<'a> = AnyColumnBufferSlice<'a>;

    fn slice(&self, valid_rows: usize) -> AnyColumnBufferSlice<'_> {
        AnyColumnBufferSlice {
            buffer: self,
            valid_rows,
        }
    }
}

/// Enables reading the valid contents of a column buffer, while it is bound to a block cursor
/// as [`BoxColumnBuffer>`].
#[derive(Clone, Copy)]
pub struct AnyColumnBufferSlice<'a> {
    buffer: &'a BoxColumnBuffer,
    valid_rows: usize,
}

impl<'a> AnyColumnBufferSlice<'a> {
    /// Fetch the associated slice if we know the underlying buffer to be of type `T`. Usually it is
    /// more convenient to use methods which some amount of knowledge about the buffer types used by
    /// this crate. I.e. it is better to use [`Self::as_text`], [`Self::as_wide_text`],
    /// [`Self::as_binary`], [`Self::as_slice`] or [`Self::as_nullable_slice`] instead. The
    /// exception would be if you use a custom column buffer type provided by your application
    /// rather than `odbc-api` itself.
    pub fn of<T>(self) -> Option<T::Slice<'a>>
    where
        T: Slice + 'static,
    {
        let buffer: &dyn Any = self.buffer.as_ref();
        let buffer = buffer.downcast_ref::<T>()?;
        Some(T::slice(buffer, self.valid_rows))
    }

    /// Use this if you know the underlying buffer holds narrow (e.g. UTF-8) character data and you
    /// want to read it.
    ///
    /// `Some` if the the underlying buffer is of type [`TextColumn`] with `u8` values. Same as
    /// `Self::of::<TextColumn<u8>>`.
    pub fn as_text(self) -> Option<TextColumnSlice<'a, u8>> {
        self.of::<TextColumn<u8>>()
    }

    /// Use this if you know the underlying buffer holds wide (i.e. UTF-16) character data and you
    /// want to read it.
    ///
    /// `Some` if the the underlying buffer is of type [`TextColumn`] with `u16` values. Same as
    /// `Self::of::<TextColumn<u16>>`.
    pub fn as_wide_text(self) -> Option<TextColumnSlice<'a, u16>> {
        self.of::<TextColumn<u16>>()
    }

    /// Use this if you know the underlying buffer holds binary data and you want to read it.
    ///
    /// `Some` if the the underlying buffer is of type [`BinColumn`]. Same as
    /// `Self::of::<BinColumn>`.
    pub fn as_binary(self) -> Option<BinColumnSlice<'a>> {
        self.of::<BinColumn>()
    }

    /// Use this if you know the underlying buffer to hold **non-nullable** data of type `T`.
    pub fn as_slice<T>(self) -> Option<&'a [T]>
    where
        T: Pod,
    {
        self.of::<Vec<T>>()
    }

    /// Use this if you know the underlying buffer to hold **nullable** data of type `T`.
    pub fn as_nullable_slice<T>(self) -> Option<NullableSlice<'a, T>>
    where
        T: Pod,
    {
        self.of::<ColumnWithIndicator<T>>()
    }
}

unsafe impl<'a> BoundInputSlice<'a> for BoxColumnBuffer {
    type SliceMut = BoxColumBufferRefMut<'a>;

    unsafe fn as_view_mut(
        &'a mut self,
        parameter_index: u16,
        stmt: StatementRef<'a>,
    ) -> BoxColumBufferRefMut<'a> {
        BoxColumBufferRefMut {
            buffer: self,
            parameter_index,
            stmt,
        }
    }
}

/// Fat reference to a [`BoxColumnBuffer`] that is bound as an array parameter to a [`Statement`].
///
/// This reference allows mutating the buffer contents, e.g. in order to reuse the same bound buffer
/// for inserting multiple batches of data. If any operation is performed which would invalidate the
/// column buffer will be automatically re-bound to the statement.
pub struct BoxColumBufferRefMut<'a> {
    /// Column buffer which will be downcast to the concrete type provided by the user in the `of`
    /// method.
    buffer: &'a mut BoxColumnBuffer,
    /// We need the parameter index to re-bind the column to the same parameter placeholder should
    /// the buffer be invalidated.
    parameter_index: u16,
    /// A handle to the statement to which the column buffer is bound. We need a reference to
    /// support operations that may invalidate the buffer. Using the statement handle we can re-bind
    /// after.
    stmt: StatementRef<'a>,
}

impl<'a> BoxColumBufferRefMut<'a> {
    /// Fetch the associated slice if we know the underlying buffer to be of type `T`.
    pub fn of<T>(self) -> Option<T::SliceMut>
    where
        T: BoundInputSlice<'a> + 'static,
    {
        let buffer: &mut dyn Any = self.buffer.as_mut();
        let buffer = buffer.downcast_mut::<T>()?;
        let view_mut = unsafe { buffer.as_view_mut(self.parameter_index, self.stmt) };
        Some(view_mut)
    }

    /// Use this if you know the underlying buffer holds wide (i.e. UTF-16) character data and you
    /// want to access it.
    ///
    /// `Some` if the the underlying buffer is of type [`TextColumn`] with `u16` values. Same as
    /// `Self::of::<TextColumn<u16>>`.
    pub fn as_wide_text(self) -> Option<TextColumnSliceMut<'a, u16>> {
        self.of::<TextColumn<u16>>()
    }

    /// Use this if you know the underlying buffer holds narrow (e.g. UTF-8) character data and you
    /// want to access it.
    ///
    /// `Some` if the the underlying buffer is of type [`TextColumn`] with `u8` values. Same as
    /// `Self::of::<TextColumn<u8>>`.
    pub fn as_text(self) -> Option<TextColumnSliceMut<'a, u8>> {
        self.of::<TextColumn<u8>>()
    }

    /// Use this if you know the underlying buffer holds binary data and you want to access it.
    ///
    /// `Some` if the the underlying buffer is of type [`BinColumn`]. Same as
    /// `Self::of::<BinColumn>`.
    pub fn as_binary(self) -> Option<BinColumnSliceMut<'a>> {
        self.of::<BinColumn>()
    }

    pub fn as_slice<T>(self) -> Option<&'a mut [T]>
    where
        T: Pod,
    {
        self.of::<Vec<T>>()
    }

    pub fn as_nullable_slice<T>(self) -> Option<NullableSliceMut<'a, T>>
    where
        T: Pod,
    {
        self.of::<ColumnWithIndicator<T>>()
    }
}

impl Resize for BoxColumnBuffer {
    fn resize(&mut self, new_capacity: usize) {
        self.as_mut().resize(new_capacity);
    }
}

#[cfg(test)]
mod tests {
    use super::{BoxColumnBuffer, BufferDesc, ColumnarDynBuffer, Slice};

    #[test]
    #[should_panic(expected = "Column indices must be unique.")]
    fn assert_unique_column_indices() {
        let bd = BufferDesc::I32 { nullable: false };
        ColumnarDynBuffer::from_descs_and_indices(1, [(1, bd), (2, bd), (1, bd)].iter().cloned());
    }

    #[test]
    fn box_column_buffer_is_resize() {
        // Given an `BoxColumnBuffer` with a capacity of 2 and values [1, 2]
        let mut buffer: BoxColumnBuffer = Box::new(vec![1i32, 2]);

        // When we resize it to a capacity of 4
        buffer.resize(4);

        // Then the buffer should still have the same values in the first two positions and the
        // remaining positions should be filled with default values (0 for i32)
        assert_eq!(buffer.slice(4).as_slice(), Some([1i32, 2, 0, 0].as_slice()));
        // And the capacity should be 4
        assert_eq!(buffer.capacity(), 4);
    }

    #[test]
    fn slice_should_only_contain_part_of_the_buffer() {
        let buffer: BoxColumnBuffer = Box::new(vec![1i32, 2, 3]);
        let view = buffer.slice(2);
        assert_eq!(Some([1, 2].as_slice()), view.as_slice::<i32>());
    }

    #[test]
    fn slice_should_be_none_if_types_mismatch() {
        let buffer: BoxColumnBuffer = Box::new(vec![1i32, 2, 3]);
        let view = buffer.slice(3);
        assert_eq!(None, view.as_slice::<i16>());
    }

    #[test]
    fn nullable_slice_should_be_none_if_buffer_is_non_nullable() {
        let buffer: BoxColumnBuffer = Box::new(vec![1i32, 2, 3]);
        let view = buffer.slice(3);
        assert!(view.as_nullable_slice::<i32>().is_none());
    }
}

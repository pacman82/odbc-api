use std::{mem, ops::Index};

use crate::{
    buffers::Indicator, fixed_sized::Pod, handles::{CDataMut, Statement, StatementRef}, parameter::VarCharArray, Error, RowSetBuffer, TruncationInfo
};

/// [`FetchRow`]s can be bound to a [`Cursor`] to enable row wise (bulk) fetching of data as opposed
/// to column wise fetching. Since all rows are bound to a C-API in a contigious block of memory the
/// row itself should be representable as such. Concretly that means that types like `String` can
/// not be supported directly by [`Row`]s for efficient bulk fetching, due to the fact it points
/// to data on the heap.
///
/// # Safety
///
/// * All the bound buffers need to be valid for the lifetime of the row.
/// * The offsets into the memory for the field representing a column, must be constant for all
///   types of the row. This is required to make the row suitable for fetching in bulk, as only the
///   first row is bound explicitly, and the bindings for all consequitive rows is calculated by
///   taking the size of the row in bytes multiplied by buffer index.
pub unsafe trait FetchRow: Copy {
    /// Binds the columns of the result set to members of the row.
    ///
    /// # Safety
    ///
    /// Caller must ensure self is alive and not moved in memory for the duration of the binding.
    unsafe fn bind_columns_to_cursor(&mut self, cursor: StatementRef<'_>) -> Result<(), Error>;

    /// If it exists, this returns the "buffer index" of a member, which has been truncated.
    fn find_truncation(&self) -> Option<TruncationInfo>;
}

/// A row wise buffer intended to be bound with [crate::Cursor::bind_buffer] in order to obtain
/// results from a cursor.
pub struct RowWiseBuffer<R> {
    /// A mutable pointer to num_rows_fetched is passed to the C-API. It is used to write back the
    /// number of fetched rows. `num_rows` is heap allocated, so the pointer is not invalidated,
    /// even if the `ColumnarBuffer` instance is moved in memory.
    num_rows: Box<usize>,
    /// Here we actually store the rows. The length of `rows` is the capacity of the `RowWiseBuffer`
    /// instance. It must not be 0.
    rows: Vec<R>,
}

impl<R> RowWiseBuffer<R> {
    /// Allocates a new Row wise buffer, which can at most `capacity` number of rows in a single
    /// call to [`crate::BlockCursor::fetch`].
    ///
    /// Panics if `capacity` is `0``.
    pub fn new(capacity: usize) -> Self
    where
        R: Default + Clone + Copy,
    {
        if capacity == 0 {
            panic!("RowWiseBuffer must have a capacity of at least `1`.")
        }
        RowWiseBuffer {
            num_rows: Box::new(0),
            rows: vec![R::default(); capacity],
        }
    }

    /// Number of valid rows in the buffer.
    pub fn num_rows(&self) -> usize {
        *self.num_rows
    }
}

impl<R> Index<usize> for RowWiseBuffer<R> {
    type Output = R;

    fn index(&self, index: usize) -> &R {
        if index >= *self.num_rows {
            panic!("Out of bounds access to RowWiseBuffer")
        }
        &self.rows[index]
    }
}

unsafe impl<R> RowSetBuffer for RowWiseBuffer<R>
where
    R: FetchRow,
{
    fn bind_type(&self) -> usize {
        mem::size_of::<R>()
    }

    fn row_array_size(&self) -> usize {
        self.rows.len()
    }

    fn mut_num_fetch_rows(&mut self) -> &mut usize {
        &mut self.num_rows
    }

    unsafe fn bind_colmuns_to_cursor(&mut self, cursor: StatementRef<'_>) -> Result<(), Error> {
        let first = self
            .rows
            .first_mut()
            .expect("rows in Row Wise buffers must not be empty.");
        first.bind_columns_to_cursor(cursor)
    }

    fn find_truncation(&self) -> Option<TruncationInfo> {
        self.rows.iter().take(*self.num_rows).find_map(|row| row.find_truncation())
    }
}

/// Can be used as a member of a [`FetchRow`] and bound to a column during row wise fetching.
///
/// # Safety
///
/// Must only be implemented for types completly representable by consequtive bytes. While members
/// can bind to Variadic types the length of the type buffering them must be known at compile time.
/// E.g. [`crate::parameter::VarCharArray`] can also bind to Variadic types but is fixed length at
/// compile time.
pub unsafe trait FetchRowMember: CDataMut + Copy{
    /// `Some` if the indicator indicates truncation. Always `None` for fixed sized types.
    fn find_truncation(&self, buffer_index: usize) -> Option<TruncationInfo> {
        self.indicator().map(|indicator| TruncationInfo {
            indicator: indicator.length(),
            buffer_index,
        })
    }

    /// Indicator for variable sized or nullable types, `None` for fixed sized types.
    fn indicator(&self) -> Option<Indicator>;

    /// Bind row element to column. Only called for the first row in a row wise buffer.
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to ensure `self` lives for the duration of the binding.
    unsafe fn bind_to_col(&mut self, col_index: u16, cursor: &mut StatementRef<'_>) -> Result<(), Error> {
        cursor.bind_col(col_index, self).into_result(cursor)
    }
}

unsafe impl<T> FetchRowMember for T where T: Pod {
    fn indicator(&self) -> Option<Indicator> {
        None
    }
}

// unsafe impl<T> FetchRowMember for Nullable<T> where T: Pod {}
unsafe impl<const LENGTH: usize> FetchRowMember for VarCharArray<LENGTH> {
    fn indicator(&self) -> Option<Indicator> {
        Some(self.indicator())
    }
}


macro_rules! impl_bind_columns_to_cursor {
    ($offset:expr, $cursor:ident,) => (
        Ok(())
    );
    ($offset:expr, $cursor:ident, $head:ident, $($tail:ident,)*) => (
        {
            $head.bind_to_col($offset, &mut $cursor)?;
            impl_bind_columns_to_cursor!($offset+1, $cursor, $($tail,)*)
        }
    );
}

macro_rules! impl_find_truncation {
    ($offset:expr,) => (
        None
    );
    ($offset:expr, $head:ident, $($tail:ident,)*) => (
        {
            if let Some(truncation_info) = $head.find_truncation($offset) {
                return Some(truncation_info);
            }
            impl_find_truncation!($offset+1, $($tail,)*)
        }
    );
}

macro_rules! impl_fetch_row_for_tuple{
    ($($t:ident)*) => (
        #[allow(unused_mut)]
        #[allow(unused_variables)]
        #[allow(non_snake_case)]
        unsafe impl<$($t:FetchRowMember,)*> FetchRow for ($($t,)*)
        {
            unsafe fn bind_columns_to_cursor(&mut self, mut cursor: StatementRef<'_>) -> Result<(), Error> {
                let ($(ref mut $t,)*) = self;
                impl_bind_columns_to_cursor!(1, cursor, $($t,)*)
            }

            fn find_truncation(&self) -> Option<TruncationInfo> {
                let ($(ref $t,)*) = self;
                impl_find_truncation!(0, $($t,)*)
            }
        }
    );
}

impl_fetch_row_for_tuple! {}
impl_fetch_row_for_tuple! { A }
impl_fetch_row_for_tuple! { A B }
impl_fetch_row_for_tuple! { A B C }
impl_fetch_row_for_tuple! { A B C D }
impl_fetch_row_for_tuple! { A B C D E }
impl_fetch_row_for_tuple! { A B C D E F }
impl_fetch_row_for_tuple! { A B C D E F G }
impl_fetch_row_for_tuple! { A B C D E F G H }
impl_fetch_row_for_tuple! { A B C D E F G H I }
impl_fetch_row_for_tuple! { A B C D E F G H I J }
impl_fetch_row_for_tuple! { A B C D E F G H I J K }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L M }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L M N }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L M N O }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L M N O P }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L M N O P Q }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L M N O P Q R }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L M N O P Q R S }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L M N O P Q R S T }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L M N O P Q R S T U }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L M N O P Q R S T U V }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L M N O P Q R S T U V W }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L M N O P Q R S T U V W X }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L M N O P Q R S T U V W X Y }
impl_fetch_row_for_tuple! { A B C D E F G H I J K L M N O P Q R S T U V W X Y Z}

#[cfg(test)]
mod tests {

    use super::RowWiseBuffer;

    #[derive(Default, Clone, Copy)]
    struct DummyRow;

    #[test]
    #[should_panic]
    fn construction_should_panic_on_capacity_zero() {
        RowWiseBuffer::<DummyRow>::new(0);
    }

    #[test]
    #[should_panic]
    fn index_should_panic_on_out_of_bound_access() {
        let buffer = RowWiseBuffer::<DummyRow>::new(1);
        // Access within the capacity of rows, but the buffer is still empty.
        let _ = buffer[0];
    }
}

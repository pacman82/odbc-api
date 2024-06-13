use std::{mem, ops::Deref};

use crate::{
    buffers::Indicator,
    handles::{CDataMut, Statement, StatementRef},
    Error, RowSetBuffer, TruncationInfo,
};

/// [`FetchRow`]s can be bound to a [`crate::Cursor`] to enable row wise (bulk) fetching of data as
/// opposed to column wise fetching. Since all rows are bound to a C-API in a contigious block of
/// memory the row itself should be representable as such. Concretly that means that types like
/// `String` can not be supported directly by [`FetchRow`]s for efficient bulk fetching, due to the
/// fact it points to data on the heap.
///
/// This trait is implement by tuples of [`FetchRowMember`]. In addition it can also be derived
/// for structs there all members implement [`FetchRowMember`] using the `Fetch` derive macro if the
/// optional derive feature is activated.
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
///
/// # Example
///
/// ```
/// use odbc_api::{Connection, Error, Cursor, parameter::VarCharArray, buffers::RowVec};
///
/// fn send_greetings(conn: &mut Connection) -> Result<(), Error> {
///     let max_rows_in_batch = 250;
///     type Row = (VarCharArray<255>, VarCharArray<255>);
///     let buffer = RowVec::<Row>::new(max_rows_in_batch);
///     let mut cursor = conn.execute("SELECT first_name, last_name FROM Persons", ())?
///         .expect("SELECT must yield a result set");
///     let mut block_cursor = cursor.bind_buffer(buffer)?;
///
///     while let Some(batch) = block_cursor.fetch()? {
///         for (first, last) in batch.iter() {
///             let first = first.as_str()
///                 .expect("First name must be UTF-8")
///                 .expect("First Name must not be NULL");
///             let last = last.as_str()
///                 .expect("Last name must be UTF-8")
///                 .expect("Last Name must not be NULL");
///             println!("Hello {first} {last}!")
///         }
///     }
///     Ok(())
/// }
/// ```
///
/// To fetch rows with this buffer type `R` must implement [`FetchRow`]. This is currently
/// implemented for tuple types. Each element of these tuples must implement [`FetchRowMember`].
///
/// Currently supported are: `f64`, `f32`, [`odbc_sys::Date`], [`odbc_sys::Timestamp`],
/// [`odbc_sys::Time`], `i16`, `u36`, `i32`, `u32`, `i8`, `u8`, `Bit`, `i64`, `u64` and
/// [`crate::parameter::VarCharArray`]. Fixed sized types can be wrapped in [`crate::Nullable`].
pub struct RowVec<R> {
    /// A mutable pointer to num_rows_fetched is passed to the C-API. It is used to write back the
    /// number of fetched rows. `num_rows` is heap allocated, so the pointer is not invalidated,
    /// even if the `ColumnarBuffer` instance is moved in memory.
    num_rows: Box<usize>,
    /// Here we actually store the rows. The length of `rows` is the capacity of the `RowWiseBuffer`
    /// instance. It must not be 0.
    rows: Vec<R>,
}

impl<R> RowVec<R> {
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
        RowVec {
            num_rows: Box::new(0),
            rows: vec![R::default(); capacity],
        }
    }

    /// Number of valid rows in the buffer.
    pub fn num_rows(&self) -> usize {
        *self.num_rows
    }
}

impl<R> Deref for RowVec<R> {
    type Target = [R];

    fn deref(&self) -> &[R] {
        &self.rows[..*self.num_rows]
    }
}

unsafe impl<R> RowSetBuffer for RowVec<R>
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
        self.rows
            .iter()
            .take(*self.num_rows)
            .find_map(|row| row.find_truncation())
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
pub unsafe trait FetchRowMember: CDataMut + Copy {
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
    unsafe fn bind_to_col(
        &mut self,
        col_index: u16,
        cursor: &mut StatementRef<'_>,
    ) -> Result<(), Error> {
        cursor.bind_col(col_index, self).into_result(cursor)
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

    use super::RowVec;

    #[derive(Default, Clone, Copy)]
    struct DummyRow;

    #[test]
    #[should_panic]
    fn construction_should_panic_on_capacity_zero() {
        RowVec::<DummyRow>::new(0);
    }

    #[test]
    #[should_panic]
    fn index_should_panic_on_out_of_bound_access() {
        let buffer = RowVec::<DummyRow>::new(1);
        // Access within the capacity of rows, but the buffer is still empty.
        let _ = buffer[0];
    }
}

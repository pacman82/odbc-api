use std::{
    collections::HashSet,
    num::NonZeroUsize,
    str::{from_utf8, Utf8Error},
};

use crate::{
    columnar_bulk_inserter::BoundInputSlice,
    cursor::TruncationInfo,
    fixed_sized::Pod,
    handles::{CDataMut, Statement, StatementRef},
    parameter::WithDataType,
    result_set_metadata::utf8_display_sizes,
    Error, ResultSetMetadata, RowSetBuffer,
};

use super::{Indicator, TextColumn};

impl<C: ColumnBuffer> ColumnarBuffer<C> {
    /// Create a new instance from columns with unique indicies. Capacity of the buffer will be the
    /// minimum capacity of the columns. The constructed buffer is always empty (i.e. the number of
    /// valid rows is considered to be zero).
    ///
    /// You do not want to call this constructor directly unless you want to provide your own buffer
    /// implentation. Most users of this crate may want to use the constructors like
    /// [`crate::buffers::ColumnarAnyBuffer::from_descs`] or
    /// [`crate::buffers::TextRowSet::from_max_str_lens`] instead.
    pub fn new(columns: Vec<(u16, C)>) -> Self {
        // Assert capacity
        let capacity = columns
            .iter()
            .map(|(_, col)| col.capacity())
            .min()
            .unwrap_or(0);

        // Assert uniqueness of indices
        let mut indices = HashSet::new();
        if columns
            .iter()
            .any(move |&(col_index, _)| !indices.insert(col_index))
        {
            panic!("Column indices must be unique.")
        }

        unsafe { Self::new_unchecked(capacity, columns) }
    }

    /// # Safety
    ///
    /// * Indices must be unique
    /// * Columns all must have enough `capacity`.
    pub unsafe fn new_unchecked(capacity: usize, columns: Vec<(u16, C)>) -> Self {
        ColumnarBuffer {
            num_rows: Box::new(0),
            row_capacity: capacity,
            columns,
        }
    }

    /// Number of valid rows in the buffer.
    pub fn num_rows(&self) -> usize {
        *self.num_rows
    }

    /// Return the number of columns in the row set.
    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    /// Use this method to gain read access to the actual column data.
    ///
    /// # Parameters
    ///
    /// * `buffer_index`: Please note that the buffer index is not identical to the ODBC column
    ///   index. For one it is zero based. It also indexes the buffer bound, and not the columns of
    ///   the output result set. This is important, because not every column needs to be bound. Some
    ///   columns may simply be ignored. That being said, if every column of the output is bound in
    ///   the buffer, in the same order in which they are enumerated in the result set, the
    ///   relationship between column index and buffer index is `buffer_index = column_index - 1`.
    pub fn column(&self, buffer_index: usize) -> C::View<'_> {
        self.columns[buffer_index].1.view(*self.num_rows)
    }
}

unsafe impl<C> RowSetBuffer for ColumnarBuffer<C>
where
    C: ColumnBuffer,
{
    fn bind_type(&self) -> usize {
        0 // Specify columnar binding
    }

    fn row_array_size(&self) -> usize {
        self.row_capacity
    }

    fn mut_num_fetch_rows(&mut self) -> &mut usize {
        self.num_rows.as_mut()
    }

    unsafe fn bind_colmuns_to_cursor(&mut self, mut cursor: StatementRef<'_>) -> Result<(), Error> {
        for (col_number, column) in &mut self.columns {
            cursor.bind_col(*col_number, column).into_result(&cursor)?;
        }
        Ok(())
    }

    fn find_truncation(&self) -> Option<TruncationInfo> {
        self.columns
            .iter()
            .enumerate()
            .find_map(|(buffer_index, (_col_index, col_buffer))| {
                col_buffer
                    .has_truncated_values(*self.num_rows)
                    .map(|indicator| TruncationInfo {
                        indicator: indicator.length(),
                        buffer_index,
                    })
            })
    }
}

/// A columnar buffer intended to be bound with [crate::Cursor::bind_buffer] in order to obtain
/// results from a cursor.
///
/// Binds to the result set column wise. This is usually helpful in dataengineering or data sciense
/// tasks. This buffer type can be used in situations there the schema of the queried data is known
/// at compile time, as well as for generic applications which do work with wide range of different
/// data.
///
/// # Example: Fetching results column wise with `ColumnarBuffer`.
///
/// Consider querying a table with two columns `year` and `name`.
///
/// ```no_run
/// use odbc_api::{
///     Environment, Cursor, ConnectionOptions,
///     buffers::{AnySlice, BufferDesc, Item, ColumnarAnyBuffer},
/// };
///
/// let env = Environment::new()?;
///
/// let batch_size = 1000; // Maximum number of rows in each row set
/// let buffer_description = [
///     // We know year to be a Nullable SMALLINT
///     BufferDesc::I16 { nullable: true },
///     // and name to be a required VARCHAR
///     BufferDesc::Text { max_str_len: 255 },
/// ];
///
/// /// Creates a columnar buffer fitting the buffer description with the capacity of `batch_size`.
/// let mut buffer = ColumnarAnyBuffer::from_descs(batch_size, buffer_description);
///
/// let mut conn = env.connect(
///     "YourDatabase", "SA", "My@Test@Password1",
///     ConnectionOptions::default(),
/// )?;
/// if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays;", ())? {
///     // Bind buffer to cursor. We bind the buffer as a mutable reference here, which makes it
///     // easier to reuse for other queries, but we could have taken ownership.
///     let mut row_set_cursor = cursor.bind_buffer(&mut buffer)?;
///     // Loop over row sets
///     while let Some(row_set) = row_set_cursor.fetch()? {
///         // Process years in row set
///         let year_col = row_set.column(0);
///         for year in i16::as_nullable_slice(year_col)
///             .expect("Year column buffer expected to be nullable Int")
///         {
///             // Iterate over `Option<i16>` with it ..
///         }
///         // Process names in row set
///         let name_col = row_set.column(1);
///         for name in name_col
///             .as_text_view()
///             .expect("Name column buffer expected to be text")
///             .iter()
///         {
///             // Iterate over `Option<&CStr> ..
///         }
///     }
/// }
/// # Ok::<(), odbc_api::Error>(())
/// ```
///
/// This second examples changes two things, we do not know the schema in advance and use the
/// SQL DataType to determine the best fit for the buffers. Also we want to do everything in a
/// function and return a `Cursor` with an already bound buffer. This approach is best if you have
/// few and very long query, so the overhead of allocating buffers is negligible and you want to
/// have an easier time with the borrow checker.
///
/// ```no_run
/// use odbc_api::{
///     Connection, BlockCursor, Error, Cursor, Nullability, ResultSetMetadata,
///     buffers::{ AnyBuffer, BufferDesc, ColumnarAnyBuffer, ColumnarBuffer }
/// };
///
/// fn get_birthdays<'a>(conn: &'a mut Connection)
///     -> Result<BlockCursor<impl Cursor + 'a, ColumnarAnyBuffer>, Error>
/// {
///     let mut cursor = conn.execute("SELECT year, name FROM Birthdays;", ())?.unwrap();
///     let mut column_description = Default::default();
///     let buffer_description : Vec<_> = (0..cursor.num_result_cols()?).map(|index| {
///         cursor.describe_col(index as u16 + 1, &mut column_description)?;
///         let nullable = matches!(
///             column_description.nullability,
///             Nullability::Unknown | Nullability::Nullable
///         );
///         let desc = BufferDesc::from_data_type(
///             column_description.data_type,
///             nullable
///         ).unwrap_or(BufferDesc::Text{ max_str_len: 255 });
///         Ok(desc)
///     }).collect::<Result<_, Error>>()?;
///
///     // Row set size of 5000 rows.
///     let buffer = ColumnarAnyBuffer::from_descs(5000, buffer_description);
///     // Bind buffer and take ownership over it.
///     cursor.bind_buffer(buffer)
/// }
/// ```
pub struct ColumnarBuffer<C> {
    /// A mutable pointer to num_rows_fetched is passed to the C-API. It is used to write back the
    /// number of fetched rows. `num_rows` is heap allocated, so the pointer is not invalidated,
    /// even if the `ColumnarBuffer` instance is moved in memory.
    num_rows: Box<usize>,
    /// aka: batch size, row array size
    row_capacity: usize,
    /// Column index and bound buffer
    columns: Vec<(u16, C)>,
}

/// A buffer for a single column intended to be used together with [`ColumnarBuffer`].
///
/// # Safety
///
/// Views must not allow access to unintialized / invalid rows.
pub unsafe trait ColumnBuffer: CDataMut {
    /// Immutable view on the column data. Used in safe abstractions. User must not be able to
    /// access uninitialized or invalid memory of the buffer through this interface.
    type View<'a>
    where
        Self: 'a;

    /// Num rows may not exceed the actual amount of valid num_rows filled by the ODBC API. The
    /// column buffer does not know how many elements were in the last row group, and therefore can
    /// not guarantee the accessed element to be valid and in a defined state. It also can not panic
    /// on accessing an undefined element.
    fn view(&self, valid_rows: usize) -> Self::View<'_>;

    /// Fills the column with the default representation of values, between `from` and `to` index.
    fn fill_default(&mut self, from: usize, to: usize);

    /// Current capacity of the column
    fn capacity(&self) -> usize;

    /// `Some` if any value is truncated in the range [0, num_rows).
    ///
    /// After fetching data we may want to know if any value has been truncated due to the buffer
    /// not being able to hold elements of that size. This method checks the indicator buffer
    /// element wise.
    fn has_truncated_values(&self, num_rows: usize) -> Option<Indicator>;
}

unsafe impl<T> ColumnBuffer for WithDataType<T>
where
    T: ColumnBuffer,
{
    type View<'a> = T::View<'a> where T: 'a;

    fn view(&self, valid_rows: usize) -> T::View<'_> {
        self.value.view(valid_rows)
    }

    fn fill_default(&mut self, from: usize, to: usize) {
        self.value.fill_default(from, to)
    }

    fn capacity(&self) -> usize {
        self.value.capacity()
    }

    fn has_truncated_values(&self, num_rows: usize) -> Option<Indicator> {
        self.value.has_truncated_values(num_rows)
    }
}

unsafe impl<'a, T> BoundInputSlice<'a> for WithDataType<T>
where
    T: BoundInputSlice<'a>,
{
    type SliceMut = T::SliceMut;

    unsafe fn as_view_mut(
        &'a mut self,
        parameter_index: u16,
        stmt: StatementRef<'a>,
    ) -> Self::SliceMut {
        self.value.as_view_mut(parameter_index, stmt)
    }
}

/// This row set binds a string buffer to each column, which is large enough to hold the maximum
/// length string representation for each element in the row set at once.
///
/// # Example
///
/// ```no_run
/// //! A program executing a query and printing the result as csv to standard out. Requires
/// //! `anyhow` and `csv` crate.
///
/// use anyhow::Error;
/// use odbc_api::{buffers::TextRowSet, Cursor, Environment, ConnectionOptions, ResultSetMetadata};
/// use std::{
///     ffi::CStr,
///     io::{stdout, Write},
///     path::PathBuf,
/// };
///
/// /// Maximum number of rows fetched with one row set. Fetching batches of rows is usually much
/// /// faster than fetching individual rows.
/// const BATCH_SIZE: usize = 5000;
///
/// fn main() -> Result<(), Error> {
///     // Write csv to standard out
///     let out = stdout();
///     let mut writer = csv::Writer::from_writer(out);
///
///     // We know this is going to be the only ODBC environment in the entire process, so this is
///     // safe.
///     let environment = unsafe { Environment::new() }?;
///
///     // Connect using a DSN. Alternatively we could have used a connection string
///     let mut connection = environment.connect(
///         "DataSourceName",
///         "Username",
///         "Password",
///         ConnectionOptions::default(),
///     )?;
///
///     // Execute a one of query without any parameters.
///     match connection.execute("SELECT * FROM TableName", ())? {
///         Some(mut cursor) => {
///             // Write the column names to stdout
///             let mut headline : Vec<String> = cursor.column_names()?.collect::<Result<_,_>>()?;
///             writer.write_record(headline)?;
///
///             // Use schema in cursor to initialize a text buffer large enough to hold the largest
///             // possible strings for each column up to an upper limit of 4KiB
///             let mut buffers = TextRowSet::for_cursor(BATCH_SIZE, &mut cursor, Some(4096))?;
///             // Bind the buffer to the cursor. It is now being filled with every call to fetch.
///             let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;
///
///             // Iterate over batches
///             while let Some(batch) = row_set_cursor.fetch()? {
///                 // Within a batch, iterate over every row
///                 for row_index in 0..batch.num_rows() {
///                     // Within a row iterate over every column
///                     let record = (0..batch.num_cols()).map(|col_index| {
///                         batch
///                             .at(col_index, row_index)
///                             .unwrap_or(&[])
///                     });
///                     // Writes row as csv
///                     writer.write_record(record)?;
///                 }
///             }
///         }
///         None => {
///             eprintln!(
///                 "Query came back empty. No output has been created."
///             );
///         }
///     }
///
///     Ok(())
/// }
/// ```
pub type TextRowSet = ColumnarBuffer<TextColumn<u8>>;

impl TextRowSet {
    /// The resulting text buffer is not in any way tied to the cursor, other than that its buffer
    /// sizes a tailor fitted to result set the cursor is iterating over.
    ///
    /// This method performs fallible buffer allocations, if no upper bound is set, so you may see
    /// a speedup, by setting an upper bound using `max_str_limit`.
    ///
    ///
    /// # Parameters
    ///
    /// * `batch_size`: The maximum number of rows the buffer is able to hold.
    /// * `cursor`: Used to query the display size for each column of the row set. For character
    ///   data the length in characters is multiplied by 4 in order to have enough space for 4 byte
    ///   utf-8 characters. This is a pessimization for some data sources (e.g. SQLite 3) which do
    ///   interpret the size of a `VARCHAR(5)` column as 5 bytes rather than 5 characters.
    /// * `max_str_limit`: Some queries make it hard to estimate a sensible upper bound and
    ///   sometimes drivers are just not that good at it. This argument allows you to specify an
    ///   upper bound for the length of character data. Any size reported by the driver is capped to
    ///   this value. In case the upper bound can not inferred by the metadata reported by the
    ///   driver the element size is set to this upper bound, too.
    pub fn for_cursor(
        batch_size: usize,
        cursor: &mut impl ResultSetMetadata,
        max_str_limit: Option<usize>,
    ) -> Result<TextRowSet, Error> {
        let buffers = utf8_display_sizes(cursor)?
            .enumerate()
            .map(|(buffer_index, reported_len)| {
                let buffer_index = buffer_index as u16;
                let col_index = buffer_index + 1;
                let max_str_len = reported_len?;
                let buffer = if let Some(upper_bound) = max_str_limit {
                    let max_str_len = max_str_len
                        .map(NonZeroUsize::get)
                        .unwrap_or(upper_bound)
                        .min(upper_bound);
                    TextColumn::new(batch_size, max_str_len)
                } else {
                    let max_str_len = max_str_len.map(NonZeroUsize::get).ok_or(
                        Error::TooLargeColumnBufferSize {
                            buffer_index,
                            num_elements: batch_size,
                            element_size: usize::MAX,
                        },
                    )?;
                    TextColumn::try_new(batch_size, max_str_len).map_err(|source| {
                        Error::TooLargeColumnBufferSize {
                            buffer_index,
                            num_elements: source.num_elements,
                            element_size: source.element_size,
                        }
                    })?
                };

                Ok::<_, Error>((col_index, buffer))
            })
            .collect::<Result<_, _>>()?;
        Ok(TextRowSet {
            row_capacity: batch_size,
            num_rows: Box::new(0),
            columns: buffers,
        })
    }

    /// Creates a text buffer large enough to hold `batch_size` rows with one column for each item
    /// `max_str_lengths` of respective size.
    pub fn from_max_str_lens(
        row_capacity: usize,
        max_str_lengths: impl IntoIterator<Item = usize>,
    ) -> Result<Self, Error> {
        let buffers = max_str_lengths
            .into_iter()
            .enumerate()
            .map(|(index, max_str_len)| {
                Ok::<_, Error>((
                    (index + 1).try_into().unwrap(),
                    TextColumn::try_new(row_capacity, max_str_len)
                        .map_err(|source| source.add_context(index.try_into().unwrap()))?,
                ))
            })
            .collect::<Result<_, _>>()?;
        Ok(TextRowSet {
            row_capacity,
            num_rows: Box::new(0),
            columns: buffers,
        })
    }

    /// Access the element at the specified position in the row set.
    pub fn at(&self, buffer_index: usize, row_index: usize) -> Option<&[u8]> {
        assert!(row_index < *self.num_rows);
        self.columns[buffer_index].1.value_at(row_index)
    }

    /// Access the element at the specified position in the row set.
    pub fn at_as_str(&self, col_index: usize, row_index: usize) -> Result<Option<&str>, Utf8Error> {
        self.at(col_index, row_index).map(from_utf8).transpose()
    }

    /// Indicator value at the specified position. Useful to detect truncation of data.
    ///
    /// # Example
    ///
    /// ```
    /// use odbc_api::buffers::{Indicator, TextRowSet};
    ///
    /// fn is_truncated(buffer: &TextRowSet, col_index: usize, row_index: usize) -> bool {
    ///     match buffer.indicator_at(col_index, row_index) {
    ///         // There is no value, therefore there is no value not fitting in the column buffer.
    ///         Indicator::Null => false,
    ///         // The value did not fit into the column buffer, we do not even know, by how much.
    ///         Indicator::NoTotal => true,
    ///         Indicator::Length(total_length) => {
    ///             // If the maximum string length is shorter than the values total length, the
    ///             // has been truncated to fit into the buffer.
    ///             buffer.max_len(col_index) < total_length
    ///         }
    ///     }
    /// }
    /// ```
    pub fn indicator_at(&self, buf_index: usize, row_index: usize) -> Indicator {
        assert!(row_index < *self.num_rows);
        self.columns[buf_index].1.indicator_at(row_index)
    }

    /// Maximum length in bytes of elements in a column.
    pub fn max_len(&self, buf_index: usize) -> usize {
        self.columns[buf_index].1.max_len()
    }
}

unsafe impl<T> ColumnBuffer for Vec<T>
where
    T: Pod,
{
    type View<'a> = &'a [T];

    fn view(&self, valid_rows: usize) -> &[T] {
        &self[..valid_rows]
    }

    fn fill_default(&mut self, from: usize, to: usize) {
        for item in &mut self[from..to] {
            *item = Default::default();
        }
    }

    fn capacity(&self) -> usize {
        self.len()
    }

    fn has_truncated_values(&self, _num_rows: usize) -> Option<Indicator> {
        None
    }
}

#[cfg(test)]
mod tests {

    use crate::buffers::{BufferDesc, ColumnarAnyBuffer};

    #[test]
    #[should_panic(expected = "Column indices must be unique.")]
    fn assert_unique_column_indices() {
        let bd = BufferDesc::I32 { nullable: false };
        ColumnarAnyBuffer::from_descs_and_indices(1, [(1, bd), (2, bd), (1, bd)].iter().cloned());
    }
}

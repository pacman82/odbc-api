use std::{
    cmp::min,
    collections::HashSet,
    str::{from_utf8, Utf8Error},
};

use crate::{
    handles::{CDataMut, HasDataType, Statement},
    parameter::WithDataType,
    parameter_collection::InputParameterCollection,
    prebound::PinnedParameterCollection,
    Cursor, Error, ParameterCollection, ResultSetMetadata, RowSetBuffer,
};

use super::{Indicator, TextColumn};

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

impl<C: ColumnBuffer> ColumnarBuffer<C> {
    /// Create a new instance from columns with unique indicies. Capacity of the buffer will be the
    /// minimum capacity of the columns. The constructed buffer is always empty (i.e. the number of
    /// valid rows is considered to be zero).
    ///
    /// You do not want to call this constructor directly unless you want to provide your own buffer
    /// implentation. Most users of this crate may want to use the constructors on
    /// [`crate::buffers::ColumnarAnyBuffer`] or [`crate::buffers::TextRowSet`] instead.
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
    ///   index. For once it is zero based. It also indexes the buffer bound, and not the columns of
    ///   the output result set. This is important, because not every column needs to be bound. Some
    ///   columns may simply be ignored. That being said, if every column of the output is bound in
    ///   the buffer, in the same order in which they are enumerated in the result set, the
    ///   relationship between column index and buffer index is `buffer_index = column_index - 1`.
    pub fn column(&self, buffer_index: usize) -> <C as ColumnProjections<'_>>::View {
        self.columns[buffer_index].1.view(*self.num_rows)
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
    /// This method is intend to be called if using [`ColumnarBuffer`] for column wise bulk inserts.
    ///
    /// ```no_run
    /// use odbc_api::{
    ///     Connection, Error, IntoParameter,
    ///     buffers::{BufferDescription, BufferKind, AnyColumnViewMut, ColumnarAnyBuffer}
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
    ///     let mut buffer = ColumnarAnyBuffer::from_description(
    ///         names.len(),
    ///         buffer_description.iter().copied()
    ///     );
    ///
    ///     // Fill the buffer with values column by column
    ///     let mut col = buffer
    ///         .column_mut(0)
    ///         .as_text_view()
    ///         .expect("We know the name column to hold text.");
    ///     col.write(names.iter().map(|s| Some(s.as_bytes())));
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
        if num_rows > self.row_capacity as usize {
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

    /// Sets the number of rows in the buffer to zero.
    pub fn clear(&mut self) {
        *self.num_rows = 0;
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

unsafe impl<C> InputParameterCollection for ColumnarBuffer<C>
where
    C: ColumnBuffer + HasDataType,
{
    fn parameter_set_size(&self) -> usize {
        *self.num_rows
    }

    unsafe fn bind_input_parameters_to(&self, stmt: &mut impl Statement) -> Result<(), Error> {
        for &(parameter_number, ref buffer) in &self.columns {
            stmt.bind_input_parameter(parameter_number, buffer)
                .into_result(stmt)?;
        }
        Ok(())
    }
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
pub struct ColumnarBuffer<C> {
    /// A mutable pointer to num_rows_fetched is passed to the C-API. It is used to write back the
    /// number of fetched rows. `num_rows_fetched` is heap allocated, so the pointer is not
    /// invalidated, even if the `ColumnarBuffer` instance is moved in memory.
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
pub unsafe trait ColumnBuffer: for<'a> ColumnProjections<'a> + CDataMut {
    /// Num rows may not exceed the actually amount of valid num_rows filled be the ODBC API. The
    /// column buffer does not know how many elements were in the last row group, and therefore can
    /// not guarantee the accessed element to be valid and in a defined state. It also can not panic
    /// on accessing an undefined element.
    fn view(&self, valid_rows: usize) -> <Self as ColumnProjections<'_>>::View;

    /// # Safety
    ///
    /// `valid_rows` must be valid, otherwise the safe abstraction would provide access to invalid
    /// memory.
    unsafe fn view_mut(&mut self, valid_rows: usize) -> <Self as ColumnProjections<'_>>::ViewMut;

    /// Fills the column with the default representation of values, between `from` and `to` index.
    fn fill_default(&mut self, from: usize, to: usize);

    /// Current capacity of the column
    fn capacity(&self) -> usize;
}

unsafe impl<'a, T> ColumnProjections<'a> for WithDataType<T>
where
    T: ColumnProjections<'a>,
{
    type View = T::View;

    type ViewMut = T::ViewMut;
}

unsafe impl<T> ColumnBuffer for WithDataType<T>
where
    T: ColumnBuffer,
{
    fn view(&self, valid_rows: usize) -> <T as ColumnProjections>::View {
        self.value.view(valid_rows)
    }

    unsafe fn view_mut(&mut self, valid_rows: usize) -> <T as ColumnProjections>::ViewMut {
        self.value.view_mut(valid_rows)
    }

    fn fill_default(&mut self, from: usize, to: usize) {
        self.value.fill_default(from, to)
    }

    fn capacity(&self) -> usize {
        self.value.capacity()
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
/// use odbc_api::{buffers::TextRowSet, Cursor, Environment, ResultSetMetadata};
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
///     )?;
///
///     // Execute a one of query without any parameters.
///     match connection.execute("SELECT * FROM TableName", ())? {
///         Some(cursor) => {
///             // Write the column names to stdout
///             let mut headline : Vec<String> = cursor.column_names()?.collect::<Result<_,_>>()?;
///             writer.write_record(headline)?;
///
///             // Use schema in cursor to initialize a text buffer large enough to hold the largest
///             // possible strings for each column up to an upper limit of 4KiB
///             let mut buffers = TextRowSet::for_cursor(BATCH_SIZE, &cursor, Some(4096))?;
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
    /// # Parameters
    ///
    /// * `batch_size`: The maximum number of rows the buffer is able to hold.
    /// * `cursor`: Used to query the display size for each column of the row set. For character
    ///   data the length in characters is multiplied by 4 in order to have enough space for 4 byte
    ///   utf-8 characters. This is a pessimization for some data sources (e.g. SQLite 3) which do
    ///   interpret the size of a `VARCHAR(5)` column as 5 bytes rather than 5 characters.
    /// * `max_str_limit`: Some queries make it hard to estimate a sensible upper bound and
    ///   sometimes drivers are just not that good at it. This argument allows you to specify an
    ///   upper bound for the length of character data.
    pub fn for_cursor(
        batch_size: usize,
        cursor: &impl ResultSetMetadata,
        max_str_len: Option<usize>,
    ) -> Result<TextRowSet, Error> {
        let num_cols: u16 = cursor.num_result_cols()?.try_into().unwrap();
        let buffers = (1..(num_cols + 1))
            .map(|col_index| {
                // Ask driver for buffer length
                let reported_len =
                    if let Some(encoded_len) = cursor.col_data_type(col_index)?.utf8_len() {
                        encoded_len
                    } else {
                        cursor.col_display_size(col_index)? as usize
                    };
                // Apply upper bound if specified
                let max_str_len = max_str_len
                    .map(|limit| min(limit, reported_len))
                    .unwrap_or(reported_len);
                Ok((
                    col_index,
                    TextColumn::try_new(batch_size, max_str_len).map_err(|source| {
                        Error::TooLargeColumnBufferSize {
                            buffer_index: col_index - 1,
                            num_elements: source.num_elements,
                            element_size: source.element_size,
                        }
                    })?,
                ))
            })
            .collect::<Result<_, Error>>()?;
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
        max_str_lengths: impl Iterator<Item = usize>,
    ) -> Result<Self, Error> {
        let buffers = max_str_lengths
            .enumerate()
            .map(|(index, max_str_len)| {
                Ok((
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
        assert!(row_index < *self.num_rows as usize);
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
        assert!(row_index < *self.num_rows as usize);
        self.columns[buf_index].1.indicator_at(row_index)
    }

    /// Maximum length in bytes of elements in a column.
    pub fn max_len(&self, buf_index: usize) -> usize {
        self.columns[buf_index].1.max_len()
    }

    /// Takes one element from the iterator for each internal column buffer and appends it to the
    /// end of the buffer. Should the buffer be not large enough to hold the element, it will be
    /// reallocated with `1.2` times its size.
    ///
    /// This method panics if it is tried to insert elements beyond batch size. It will also panic
    /// if row does not contain at least one item for each internal column buffer.
    pub fn append<'a>(&mut self, mut row: impl Iterator<Item = Option<&'a [u8]>>) {
        if self.row_capacity == *self.num_rows {
            panic!("Trying to insert elements into TextRowSet beyond batch size.")
        }

        let index = *self.num_rows;
        for (_, column) in &mut self.columns {
            let text = row.next().expect(
                "Row passed to TextRowSet::append must contain one element for each column.",
            );
            column.append(index, text);
        }

        *self.num_rows += 1;
    }
}

/// Since every pointer we bind, including the one storing the number of returned rows is head
/// allocated, ColumnarBuffer is safe to move without invalidating pointers. We express this by
/// implemneting this trait.
unsafe impl<C> PinnedParameterCollection for ColumnarBuffer<C>
where
    ColumnarBuffer<C>: ParameterCollection,
{
    type Target = Self;

    unsafe fn deref(&mut self) -> &mut Self::Target {
        self
    }
}

#[cfg(test)]
mod tests {

    use crate::buffers::ColumnarAnyBuffer;

    use super::super::{BufferDescription, BufferKind};

    #[test]
    #[should_panic(expected = "Column indices must be unique.")]
    fn assert_unique_column_indices() {
        let bd = BufferDescription {
            nullable: false,
            kind: BufferKind::I32,
        };
        ColumnarAnyBuffer::from_description_and_indices(
            1,
            [(1, bd), (2, bd), (1, bd)].iter().cloned(),
        );
    }
}

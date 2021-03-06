use super::{text_column::TextColumn, Indicator};
use crate::{handles::Statement, Cursor, Error, ParameterCollection, RowSetBuffer};
use std::{
    cmp::min,
    str::{from_utf8, Utf8Error},
};

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
/// use odbc_api::{buffers::TextRowSet, Cursor, Environment};
/// use std::{
///     ffi::CStr,
///     io::{stdout, Write},
///     path::PathBuf,
/// };
///
/// /// Maximum number of rows fetched with one row set. Fetching batches of rows is usually much
/// /// faster than fetching individual rows.
/// const BATCH_SIZE: u32 = 100000;
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
pub struct TextRowSet {
    // Current implementation is straight forward. We could consider allocating one block of memory
    // in allocation instead.
    batch_size: u32,
    /// A mutable pointer to num_rows_fetched is passed to the C-API. It is used to write back the
    /// number of fetched rows. `num_rows_fetched` is heap allocated, so the pointer is not
    /// invalidated, even if the `TextRowSet` instance is moved in memory.
    num_rows: Box<usize>,
    /// Text buffers for the individual columns.
    buffers: Vec<TextColumn<u8>>,
}

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
        batch_size: u32,
        cursor: &impl Cursor,
        max_str_len: Option<usize>,
    ) -> Result<TextRowSet, Error> {
        let num_cols = cursor.num_result_cols()?;
        let buffers = (1..(num_cols + 1))
            .map(|col_index| {
                // Ask driver for buffer length
                let reported_len =
                    if let Some(encoded_len) = cursor.col_data_type(col_index as u16)?.utf8_len() {
                        encoded_len
                    } else {
                        cursor.col_display_size(col_index as u16)? as usize
                    };
                // Apply upper bound if specified
                let max_str_len = max_str_len
                    .map(|limit| min(limit, reported_len))
                    .unwrap_or(reported_len);
                Ok(TextColumn::new(batch_size as usize, max_str_len))
            })
            .collect::<Result<_, Error>>()?;
        Ok(TextRowSet {
            batch_size,
            num_rows: Box::new(0),
            buffers,
        })
    }

    /// Creates a text buffer large enough to hold `batch_size` rows with one column for each item
    /// `max_str_lengths` of respective size.
    pub fn new(batch_size: u32, max_str_lengths: impl Iterator<Item = usize>) -> Self {
        let buffers = max_str_lengths
            .map(|max_str_len| TextColumn::new(batch_size as usize, max_str_len))
            .collect();
        TextRowSet {
            batch_size,
            num_rows: Box::new(0),
            buffers,
        }
    }

    /// Access the element at the specified position in the row set.
    pub fn at(&self, col_index: usize, row_index: usize) -> Option<&[u8]> {
        assert!(row_index < *self.num_rows as usize);
        unsafe { self.buffers[col_index].value_at(row_index) }
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
    pub fn indicator_at(&self, col_index: usize, row_index: usize) -> Indicator {
        assert!(row_index < *self.num_rows as usize);
        unsafe { self.buffers[col_index].indicator_at(row_index) }
    }

    /// Return the number of columns in the row set.
    pub fn num_cols(&self) -> usize {
        self.buffers.len()
    }

    /// Return the number of rows in the row set.
    pub fn num_rows(&self) -> usize {
        *self.num_rows as usize
    }

    /// Maximum length in bytes of elemenst in a column.
    pub fn max_len(&self, col_index: usize) -> usize {
        self.buffers[col_index].max_len()
    }

    /// Takes one element from the iterator for each internal column buffer and appends it to the
    /// end of the buffer. Should the buffer be not large enough to hold the element, it will be
    /// reallocated with `1.2` times its size.
    ///
    /// This method panics if it is tried to insert elements beyond batch size. It will also panic
    /// if row does not contain at least one item for each internal column buffer.
    pub fn append<'a>(&mut self, mut row: impl Iterator<Item = Option<&'a [u8]>>) {
        if self.batch_size == *self.num_rows as u32 {
            panic!("Trying to insert elements into TextRowSet beyond batch size.")
        }

        let index = *self.num_rows;
        for column in &mut self.buffers {
            let text = row.next().expect(
                "Row passed to TextRowSet::append must contain one element for each column.",
            );
            column.append(index, text);
        }

        *self.num_rows += 1;
    }

    /// Sets the number of rows in the buffer to zero.
    pub fn clear(&mut self) {
        *self.num_rows = 0;
    }
}

unsafe impl RowSetBuffer for TextRowSet {
    fn bind_type(&self) -> u32 {
        0 // Specify column wise binding.
    }

    fn row_array_size(&self) -> u32 {
        self.batch_size
    }

    fn mut_num_fetch_rows(&mut self) -> &mut usize {
        self.num_rows.as_mut()
    }

    unsafe fn bind_to_cursor(&mut self, cursor: &mut impl Cursor) -> Result<(), Error> {
        for (index, column_buffer) in self.buffers.iter_mut().enumerate() {
            let column_number = (index + 1) as u16;
            cursor.stmt().bind_col(column_number, column_buffer)?;
        }
        Ok(())
    }
}

unsafe impl ParameterCollection for &TextRowSet {
    fn parameter_set_size(&self) -> u32 {
        *self.num_rows as u32
    }

    unsafe fn bind_parameters_to(
        self,
        stmt: &mut crate::handles::StatementImpl,
    ) -> Result<(), Error> {
        let mut parameter_number = 1;
        for column in &self.buffers {
            stmt.bind_input_parameter(parameter_number, column)?;
            parameter_number += 1;
        }
        Ok(())
    }
}

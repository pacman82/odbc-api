use odbc_sys::{len_data_at_exec, CDataType, DATA_AT_EXEC};

use crate::{
    handles::{DelayedInput, HasDataType, Statement},
    DataType, Error, ParameterCollection, ParameterTupleElement,
};
use std::{
    ffi::c_void,
    fs::File,
    io::{self, BufRead, BufReader},
    num::NonZeroUsize,
    path::Path,
};

/// A `Blob` can stream its contents to the database batch by batch and may therefore be used to
/// transfer large amounts of data, exceeding the drivers capabilities for normal input parameters.
///
/// # Safety
///
/// If a hint is implemented for `blob_size` it must be accurate before the first call to
/// `next_batch`.
pub unsafe trait Blob: HasDataType {
    /// CData type of the binary data returned in the batches. Likely to be either
    /// [`crate::sys::CDataType::Binary`], [`crate::sys::CDataType::Char`] or
    /// [`crate::sys::CDataType::WChar`].
    fn c_data_type(&self) -> CDataType;

    /// Hint passed on to the driver regarding the combined size of all the batches. This hint is
    /// passed then the parameter is bound to the statement, so its meaning is only defined before
    /// the first call to `next_batch`. If `None` no hint about the total length of the batches is
    /// passed to the driver and the indicator will be set to [`crate::sys::DATA_AT_EXEC`].
    fn size_hint(&self) -> Option<usize>;

    /// Retrieve the next batch of data from the source. Batches may not be empty. `None` indicates
    /// the last batch has been reached.
    fn next_batch(&mut self) -> io::Result<Option<&[u8]>>;

    /// Convinience function. Same as calling [`self::BlobParam::new`].
    fn as_blob_param(&mut self) -> BlobParam
    where
        Self: Sized,
    {
        BlobParam::new(self)
    }
}

/// Parameter type which can be used to bind a [`self::Blob`] as parameter to a statement in order
/// for its contents to be streamed to the database at statement execution time.
pub struct BlobParam<'a> {
    /// Should be [`crate::sys::DATA_AT_EXEC`] if no size hint is given, or the result of
    /// [`crate::sys::len_data_at_exec`].
    indicator: isize,
    /// Trait object to be bound as a delayed parameter.
    blob: &'a mut dyn Blob,
}

impl<'a> BlobParam<'a> {
    pub fn new(blob: &'a mut impl Blob) -> Self {
        let indicator = if let Some(size) = blob.size_hint() {
            len_data_at_exec(size.try_into().unwrap())
        } else {
            DATA_AT_EXEC
        };
        Self { indicator, blob }
    }
}

unsafe impl DelayedInput for BlobParam<'_> {
    fn cdata_type(&self) -> CDataType {
        self.blob.c_data_type()
    }

    fn indicator_ptr(&self) -> *const isize {
        &self.indicator as *const isize
    }

    fn stream_ptr(&mut self) -> *mut c_void {
        // Types must have the same size for the transmute to work in the reverse cast.
        debug_assert_eq!(
            std::mem::size_of::<*mut &mut dyn Blob>(),
            std::mem::size_of::<*mut c_void>()
        );
        &mut self.blob as *mut &mut dyn Blob as *mut c_void
    }
}

impl HasDataType for BlobParam<'_> {
    fn data_type(&self) -> DataType {
        self.blob.data_type()
    }
}

unsafe impl ParameterCollection for BlobParam<'_> {
    fn parameter_set_size(&self) -> usize {
        1
    }

    unsafe fn bind_parameters_to(&mut self, stmt: &mut impl Statement) -> Result<(), Error> {
        stmt.bind_delayed_input_parameter(1, self).into_result(stmt)
    }
}

unsafe impl ParameterTupleElement for &mut BlobParam<'_> {
    unsafe fn bind_to(
        &mut self,
        parameter_number: u16,
        stmt: &mut impl Statement,
    ) -> Result<(), Error> {
        stmt.bind_delayed_input_parameter(parameter_number, *self)
            .into_result(stmt)
    }
}

/// Wraps borrowed bytes with a batch_size and implements [`self::Blob`]. Use this type to send long
/// array of bytes to the database.
pub struct BlobSlice<'a> {
    /// If `true` the blob is going to be bound as [`DataType::LongVarbinary`] and the bytes are
    /// interpreted as [`CDataType::Binary`]. If false the blob is going to be bound as
    /// [`DataType::LongVarchar`] and the bytes are interpreted as [`CDataType::Char`].
    pub is_binary: bool,
    /// Maximum number of bytes transferred to the database in one go. May be largere than the
    /// remaining blob size.
    pub batch_size: usize,
    /// Remaining bytes to transfer to the database.
    pub blob: &'a [u8],
}

impl<'a> BlobSlice<'a> {
    /// Construct a Blob from a byte slice. The blob is going to be bound as a `LongVarbinary` and
    /// will be transmitted in one batch.
    ///
    /// # Example
    ///
    /// ```
    /// use odbc_api::{Connection, parameter::{Blob, BlobSlice}, IntoParameter, Error};
    ///
    /// fn insert_image(
    ///     conn: &Connection<'_>,
    ///     id: &str,
    ///     image_data: &[u8]
    /// ) -> Result<(), Error>
    /// {
    ///     let mut blob = BlobSlice::from_byte_slice(image_data);
    ///
    ///     let insert = "INSERT INTO Images (id, image_data) VALUES (?,?)";
    ///     let parameters = (&id.into_parameter(), &mut blob.as_blob_param());
    ///     conn.execute(&insert, parameters)?;
    ///     Ok(())
    /// }
    /// ```
    pub fn from_byte_slice(blob: &'a [u8]) -> Self {
        Self {
            is_binary: true,
            batch_size: blob.len(),
            blob,
        }
    }

    /// Construct a Blob from a text slice. The blob is going to be bound as a `LongVarchar` and
    /// will be transmitted in one batch.
    ///
    /// # Example
    ///
    /// This example insert `title` as a normal input parameter but streams the potentially much
    /// longer `String` in `text` to the database as a large text blob. This allows to circumvent
    /// the size restrictions for `String` arguments of many drivers (usually around 4 or 8 KiB).
    ///
    /// ```
    /// use odbc_api::{Connection, parameter::{Blob, BlobSlice}, IntoParameter, Error};
    ///
    /// fn insert_book(
    ///     conn: &Connection<'_>,
    ///     title: &str,
    ///     text: &str
    /// ) -> Result<(), Error>
    /// {
    ///     let mut blob = BlobSlice::from_text(&text);
    ///
    ///     let insert = "INSERT INTO Books (title, text) VALUES (?,?)";
    ///     let parameters = (&title.into_parameter(), &mut blob.as_blob_param());
    ///     conn.execute(&insert, parameters)?;
    ///     Ok(())
    /// }
    /// ```
    pub fn from_text(text: &'a str) -> Self {
        Self {
            is_binary: false,
            batch_size: text.len(),
            blob: text.as_bytes(),
        }
    }
}

impl HasDataType for BlobSlice<'_> {
    fn data_type(&self) -> DataType {
        if self.is_binary {
            DataType::LongVarbinary {
                length: NonZeroUsize::new(self.blob.len()),
            }
        } else {
            DataType::LongVarchar {
                length: NonZeroUsize::new(self.blob.len()),
            }
        }
    }
}

unsafe impl Blob for BlobSlice<'_> {
    fn c_data_type(&self) -> CDataType {
        if self.is_binary {
            CDataType::Binary
        } else {
            CDataType::Char
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.blob.len())
    }

    fn next_batch(&mut self) -> io::Result<Option<&[u8]>> {
        if self.blob.is_empty() {
            return Ok(None);
        }

        if self.blob.len() >= self.batch_size {
            let (head, tail) = self.blob.split_at(self.batch_size);
            self.blob = tail;
            Ok(Some(head))
        } else {
            let last_batch = self.blob;
            self.blob = &[];
            Ok(Some(last_batch))
        }
    }
}

/// Wraps an [`std::io::BufRead`] and implements [`self::Blob`]. Use this to stream contents from an
/// [`std::io::BufRead`] to the database. The blob implementation is going to directly utilize the
/// Buffer of the [`std::io::BufRead`] implementation, so the batch size is likely equal to that
/// capacity.
pub struct BlobRead<R> {
    /// `true` if `size` is to interpreted as the exact ammount of bytes contained in the reader, at
    /// the time of binding it as a parameter. `false` if `size` is to be interpreted as an upper
    /// bound.
    exact: bool,
    size: usize,
    consume: usize,
    buf_read: R,
}

impl<R> BlobRead<R> {
    /// Construct a blob read from any [`std::io::BufRead`]. The `upper bound` is used in the type
    /// description then binding the blob as a parameter.
    ///
    /// # Examples
    ///
    /// This is more flexible than [`Self::from_path`]. Note however that files provide metadata
    /// about the length of the data, which `io::BufRead` does not. This is not an issue for most
    /// drivers, but some can perform optimization if they know the size in advance. In the tests
    /// SQLite has shown a bug to only insert empty data if no size hint has been provided.
    ///
    /// ```
    /// use std::io::BufRead;
    /// use odbc_api::{Connection, parameter::{Blob, BlobRead}, IntoParameter, Error};
    ///
    /// fn insert_image_to_db(
    ///     conn: &Connection<'_>,
    ///     id: &str,
    ///     image_data: impl BufRead) -> Result<(), Error>
    /// {
    ///     const MAX_IMAGE_SIZE: usize = 4 * 1024 * 1024;
    ///     let mut blob = BlobRead::with_upper_bound(image_data, MAX_IMAGE_SIZE);
    ///
    ///     let sql = "INSERT INTO Images (id, image_data) VALUES (?, ?)";
    ///     let parameters = (&id.into_parameter(), &mut blob.as_blob_param());
    ///     conn.execute(sql, parameters)?;
    ///     Ok(())
    /// }
    /// ```
    pub fn with_upper_bound(buf_read: R, upper_bound: usize) -> Self {
        Self {
            exact: false,
            consume: 0,
            size: upper_bound,
            buf_read,
        }
    }

    /// Construct a blob read from any [`std::io::BufRead`]. The `upper bound` is used in the type
    /// description then binding the blob as a parameter and is also passed to indicate the size
    /// of the actual value to the ODBC driver.
    ///
    /// # Safety
    ///
    /// The ODBC driver may use the exact size hint to allocate buffers internally. Too short may
    /// lead to invalid writes and too long may lead to invalid reads, so to be save the hint must
    /// be exact.
    pub unsafe fn with_exact_size(buf_read: R, exact_size: usize) -> Self {
        Self {
            exact: true,
            consume: 0,
            size: exact_size,
            buf_read,
        }
    }
}

impl BlobRead<BufReader<File>> {
    /// Construct a blob from a Path. The metadata of the file is used to give the ODBC driver a
    /// size hint.
    ///
    /// # Example
    ///
    /// [`BlobRead::from_path`] is the most convenient way to turn a file path into a [`Blob`]
    /// parameter. The following example also demonstrates that the streamed blob parameter can be
    /// combined with reqular input parmeters like `id`.
    ///
    /// ```
    /// use std::{error::Error, path::Path};
    /// use odbc_api::{Connection, parameter::{Blob, BlobRead}, IntoParameter};
    ///
    /// fn insert_image_to_db(
    ///     conn: &Connection<'_>,
    ///     id: &str,
    ///     image_path: &Path) -> Result<(), Box<dyn Error>>
    /// {
    ///     let mut blob = BlobRead::from_path(&image_path)?;
    ///
    ///     let sql = "INSERT INTO Images (id, image_data) VALUES (?, ?)";
    ///     let parameters = (&id.into_parameter(), &mut blob.as_blob_param());
    ///     conn.execute(sql, parameters)?;
    ///     Ok(())
    /// }
    /// ```
    pub fn from_path(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let size = file.metadata()?.len().try_into().unwrap();
        let buf_read = BufReader::new(file);
        Ok(Self {
            consume: 0,
            exact: true,
            size,
            buf_read,
        })
    }
}

impl<R> HasDataType for BlobRead<R>
where
    R: BufRead,
{
    fn data_type(&self) -> DataType {
        DataType::LongVarbinary {
            length: NonZeroUsize::new(self.size),
        }
    }
}

unsafe impl<R> Blob for BlobRead<R>
where
    R: BufRead,
{
    fn c_data_type(&self) -> CDataType {
        CDataType::Binary
    }

    fn size_hint(&self) -> Option<usize> {
        if self.exact {
            Some(self.size)
        } else {
            None
        }
    }

    fn next_batch(&mut self) -> io::Result<Option<&[u8]>> {
        if self.consume != 0 {
            self.buf_read.consume(self.consume);
        }
        let batch = self.buf_read.fill_buf()?;
        self.consume = batch.len();
        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }
}

use odbc_sys::{len_data_at_exec, CDataType, DATA_AT_EXEC};

use crate::{
    handles::{DelayedInput, HasDataType, Statement, StatementImpl},
    DataType, Error, Parameter,
};
use std::{convert::TryInto, ffi::c_void, io};

/// A `Blob` can stream its contents to the database batch by batch and may therefore be used to
/// transfer large amounts of data, exceeding the drivers capabilities for normal input parameters.
///
/// # Safety
///
/// If a hint is implemented for `blob_size` it must be accurate before the first call to
/// `next_batch`.
pub unsafe trait Blob: HasDataType {
    /// Hint passed on to the driver regarding the combined size of all the batches. This hint is
    /// passed then the parameter is bound to the statement, so its meaning is only defined before
    /// the first call to `next_batch`. If `None` no hint about the total length of the batches is
    /// passed to the driver and the indicator will be set to [`crate::sys::DATA_AT_EXEC`].
    fn size_hint(&self) -> Option<usize>;

    /// Retrieve the netxt batch of data from the source. Batches may not be empty. `None` indicates
    /// the last batch has been reached.
    fn next_batch(&mut self) -> io::Result<Option<&[u8]>>;

    /// Convinience function. Same as calling [`self::BlobParam::new`].
    fn as_param(&mut self) -> BlobParam where Self: Sized {
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
        CDataType::Binary
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

unsafe impl HasDataType for BlobParam<'_> {
    fn data_type(&self) -> DataType {
        self.blob.data_type()
    }
}

unsafe impl Parameter for &mut BlobParam<'_> {
    unsafe fn bind_parameter(
        self,
        parameter_number: u16,
        stmt: &mut StatementImpl<'_>,
    ) -> Result<(), Error> {
        stmt.bind_delayed_input_parameter(parameter_number, self)
    }
}

/// Wraps borrowed bytes with a batch_size and implements [`self::Blob`].
pub struct BinaryBatches<'a> {
    /// Maximum number of bytes transferred to the database in one go.
    pub batch_size: usize,
    /// Remaining bytes to transfer to the database.
    pub blob: &'a [u8],
}

unsafe impl HasDataType for BinaryBatches<'_> {
    fn data_type(&self) -> DataType {
        DataType::LongVarbinary {
            length: self.blob.len(),
        }
    }
}

unsafe impl Blob for BinaryBatches<'_> {
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
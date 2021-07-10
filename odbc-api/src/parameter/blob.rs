use std::convert::TryInto;

use crate::{handles::Statement, DataType, Parameter};

/// Can stream a sequence of binary batches. Use this to put large data.
///
/// # Safety
///
/// Should a size hint be implemented (i.e. `len` returns `Some` value), then it must be correct.
pub unsafe trait BlobInputStream {
    /// Fetches the next batch from the stream. The batch may not be valid once the next call to
    /// `next` (as enforced by the signature).
    fn next(&mut self) -> Option<&[u8]>;

    /// Total length of all batches combined. If available this information will be send to the
    /// driver before the first call to next. It depends on the driver wether or not something
    /// clever happens with this.
    fn len(&self) -> Option<usize>;

    /// SQL Parameter Data Type as which the stream is going to be bound. E.g.
    /// [`DataType::LongVarchar`] for narrow text data.
    fn data_type(&self) -> DataType;
}

/// Can bind stream input data at statement execution time. Useful for sending long data to the
/// connected data source.
pub struct BlobInputParameter<B> {
    stream: B,
    indicator: isize,
}

impl<B> BlobInputParameter<B>
where
    B: BlobInputStream,
{
    /// Create a new blob input parameter from a blob stream.
    pub fn new(stream: B) -> Self {
        let indicator = if let Some(len) = stream.len() {
            odbc_sys::len_data_at_exec(len.try_into().unwrap())
        } else {
            odbc_sys::DATA_AT_EXEC
        };

        Self { stream, indicator }
    }
}

unsafe impl<B> Parameter for &mut BlobInputParameter<B>
where
    B: BlobInputStream,
{
    unsafe fn bind_parameter(
        self,
        parameter_number: u16,
        stmt: &mut crate::handles::StatementImpl<'_>,
    ) -> Result<(), crate::Error> {
        stmt.bind_input_blob(
            parameter_number,
            parameter_number as odbc_sys::Pointer,
            self.stream.data_type(),
            &self.indicator,
        )
    }
}

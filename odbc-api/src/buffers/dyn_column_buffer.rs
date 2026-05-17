use super::{ColumnBuffer, Indicator, Slice};

use crate::{
    buffers::ColumnarBuffer,
    handles::{CData, CDataMut},
};

use std::{any::Any, ffi::c_void};

/// Columnar buffer with dynamic column types decided at runtime.
///
/// Go to buffer implementation for this crate if fetching data. If you have no reason to use
/// something else use this.
pub type ColumnarDynBuffer = ColumnarBuffer<BoxColumnBuffer>;

/// Heap allocated column buffer with dynamic type. Intended to be used as a type parameter for
/// [`ColumnarBuffer`] to enable columns of different types only known at runtime to be bound to the
/// same block cursor.
pub type BoxColumnBuffer = Box<dyn AnyColumnBuffer>;

/// A [`ColumnBuffer`] that additionally carries runtime type information, so a trait object can
/// be downcast to the concrete buffer type it was constructed from. Intended for [`ColumnarBuffer`]
/// with heterogenous columns, i.e. integer and text columns.
pub trait AnyColumnBuffer: ColumnBuffer + Any {}

impl<T> AnyColumnBuffer for T where T: ColumnBuffer + Any {}

unsafe impl CData for Box<dyn AnyColumnBuffer> {
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

unsafe impl CDataMut for Box<dyn AnyColumnBuffer> {
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        self.as_mut().mut_indicator_ptr()
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.as_mut().mut_value_ptr()
    }
}

unsafe impl ColumnBuffer for Box<dyn AnyColumnBuffer> {
    fn capacity(&self) -> usize {
        self.as_ref().capacity()
    }

    fn has_truncated_values(&self, num_rows: usize) -> Option<Indicator> {
        self.as_ref().has_truncated_values(num_rows)
    }
}

unsafe impl Slice for Box<dyn AnyColumnBuffer> {
    type Slice<'a> = AnyColumnBufferSlice<'a>;

    fn slice(&self, valid_rows: usize) -> AnyColumnBufferSlice<'_> {
        AnyColumnBufferSlice {
            buffer: self,
            valid_rows,
        }
    }
}

/// Enables reading the valid contents of a column buffer, while it is bound to a block cursor
/// as [`Box<dyn AnyColumnBuffer>`].
pub struct AnyColumnBufferSlice<'a> {
    buffer: &'a dyn Any,
    valid_rows: usize,
}

impl<'a> AnyColumnBufferSlice<'a> {
    pub fn of<T>(&self) -> Option<T::Slice<'_>>
    where
        T: Slice + 'static,
    {
        let buffer: &dyn Any = self.buffer;
        let buffer = buffer.downcast_ref::<T>()?;
        Some(T::slice(buffer, self.valid_rows))
    }
}

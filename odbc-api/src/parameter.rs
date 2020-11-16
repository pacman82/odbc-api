use std::{convert::TryInto, ffi::c_void};

use odbc_sys::{CDataType, Len};

use crate::{DataType, handles::{CData, Input}};

/// Extend the input trait with the guarantee, that the bound parameter buffer contains at least one
/// element.
pub unsafe trait Parameter : Input {}

/// Annotates an instance of an inner type with an SQL Data type in order to indicate how it should
/// be bound as a parameter to an SQL Statement.
pub struct WithDataType<T> {
    pub value: T,
    pub data_type: DataType,
}

unsafe impl<T> CData for WithDataType<T>
where
    T: CData,
{
    fn cdata_type(&self) -> CDataType {
        self.value.cdata_type()
    }

    fn indicator_ptr(&self) -> *const Len {
        self.value.indicator_ptr()
    }

    fn value_ptr(&self) -> *const c_void {
        self.value.value_ptr()
    }
}

unsafe impl<T> Input for WithDataType<T>
where
    T: Input,
{
    fn data_type(&self) -> DataType {
        self.data_type
    }
}

unsafe impl<T> Parameter for WithDataType<T> where T: Parameter {}

/// Binds a byte array as a VarChar input parameter.
///
/// While a byte array can provide us with a pointer to the start of the array and the length of the
/// array itself, it can not provide us with a pointer to the length of the buffer. So to bind
/// strings which are not zero terminated we need to store the length in a separate value.
pub struct VarChar<'a> {
    bytes: &'a [u8],
    /// Will be set to value.len() by constructor.
    length: Len,
}

impl<'a> VarChar<'a> {
    pub fn new(value: &'a [u8]) -> Self {
        VarChar {
            bytes: value,
            length: value.len().try_into().unwrap(),
        }
    }
}

unsafe impl CData for VarChar<'_> {
    fn cdata_type(&self) -> CDataType {
        CDataType::Char
    }

    fn indicator_ptr(&self) -> *const Len {
        &self.length
    }

    fn value_ptr(&self) -> *const c_void {
        self.bytes.as_ptr() as *const c_void
    }
}

unsafe impl Input for VarChar<'_> {
    fn data_type(&self) -> DataType {
        DataType::Varchar {
            length: self.bytes.len().try_into().unwrap(),
        }
    }
}

unsafe impl Parameter for VarChar<'_> {}
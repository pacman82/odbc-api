//! Module contains trait implementations for `CStr` and `CString`.

use std::ffi::{c_void, CStr, CString};

use odbc_sys::{CDataType, NTS};

use crate::{
    handles::{CData, HasDataType},
    parameter::InputParameter,
    DataType,
};

unsafe impl CData for CStr {
    fn cdata_type(&self) -> CDataType {
        CDataType::Char
    }

    fn indicator_ptr(&self) -> *const isize {
        &NTS as *const isize
    }

    fn value_ptr(&self) -> *const c_void {
        self.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> isize {
        0
    }
}

impl HasDataType for CStr {
    fn data_type(&self) -> DataType {
        DataType::Varchar {
            length: self.to_bytes().len(),
        }
    }
}

unsafe impl InputParameter for CStr {}

unsafe impl CData for CString {
    fn cdata_type(&self) -> CDataType {
        CDataType::Char
    }

    fn indicator_ptr(&self) -> *const isize {
        &NTS as *const isize
    }

    fn value_ptr(&self) -> *const c_void {
        self.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> isize {
        0
    }
}

impl HasDataType for CString {
    fn data_type(&self) -> DataType {
        DataType::Varchar {
            length: self.as_bytes().len(),
        }
    }
}

unsafe impl InputParameter for CString {}

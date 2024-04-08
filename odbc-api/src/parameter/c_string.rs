//! Module contains trait implementations for `CStr` and `CString`.

use std::{
    ffi::{c_void, CStr, CString},
    num::NonZeroUsize,
};

use odbc_sys::{CDataType, NTS};

use crate::{
    handles::{CData, HasDataType},
    parameter::InputParameter,
    DataType,
};

use super::CElement;

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
            length: NonZeroUsize::new(self.to_bytes().len()),
        }
    }
}
unsafe impl CElement for CStr {
    /// `CStr` is terminated by zero. Indicator can therefore never indicate a truncated value and
    /// is always complete.
    fn assert_completness(&self) {}
}
impl InputParameter for CStr {}

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
            length: NonZeroUsize::new(self.as_bytes().len()),
        }
    }
}

unsafe impl CElement for CString {
    /// `CString`` is terminated by zero. Indicator can therefore never indicate a truncated value
    /// and is always complete.
    fn assert_completness(&self) {}
}

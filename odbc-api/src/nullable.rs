use std::ffi::c_void;

use odbc_sys::NULL_DATA;

use crate::{
    buffers::{FetchRowMember, Indicator},
    fixed_sized::Pod,
    handles::{CData, CDataMut, HasDataType},
    parameter::CElement,
    OutputParameter,
};

/// Wraps a type T together with an additional indicator. This way the type gains a Null
/// representation, those memory layout is compatible with ODBC.
#[derive(Clone, Copy)]
pub struct Nullable<T> {
    indicator: isize,
    value: T,
}

impl<T> Nullable<T> {
    pub fn new(value: T) -> Self {
        Self {
            indicator: 0,
            value,
        }
    }

    pub fn null() -> Self
    where
        T: Default,
    {
        Self {
            indicator: NULL_DATA,
            value: Default::default(),
        }
    }

    pub fn as_opt(&self) -> Option<&T> {
        if self.indicator == NULL_DATA {
            None
        } else {
            Some(&self.value)
        }
    }

    pub fn into_opt(self) -> Option<T> {
        if self.indicator == NULL_DATA {
            None
        } else {
            Some(self.value)
        }
    }
}

impl<T> Default for Nullable<T>
where
    T: Default,
{
    fn default() -> Self {
        Self::null()
    }
}

unsafe impl<T> CData for Nullable<T>
where
    T: Pod,
{
    fn cdata_type(&self) -> odbc_sys::CDataType {
        self.value.cdata_type()
    }

    fn indicator_ptr(&self) -> *const isize {
        &self.indicator as *const isize
    }

    fn value_ptr(&self) -> *const c_void {
        self.value.value_ptr()
    }

    fn buffer_length(&self) -> isize {
        0
    }
}

impl<T> HasDataType for Nullable<T>
where
    T: Pod + HasDataType,
{
    fn data_type(&self) -> crate::DataType {
        self.value.data_type()
    }
}

unsafe impl<T> CElement for Nullable<T>
where
    T: Pod,
{
    /// Does nothing. `T` is fixed size and therfore always complete.
    fn assert_completness(&self) {}
}

unsafe impl<T> CDataMut for Nullable<T>
where
    T: Pod,
{
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        &mut self.indicator as *mut isize
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        &mut self.value as *mut T as *mut c_void
    }
}

unsafe impl<T> OutputParameter for Nullable<T> where T: Pod + HasDataType {}

unsafe impl<T> FetchRowMember for Nullable<T>
where
    T: Pod,
{
    fn indicator(&self) -> Option<crate::buffers::Indicator> {
        Some(Indicator::from_isize(self.indicator))
    }
}

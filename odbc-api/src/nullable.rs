use std::ffi::c_void;

use odbc_sys::NULL_DATA;

use crate::{
    fixed_sized::FixedSizedCType,
    handles::{CData, CDataMut, HasDataType},
    Parameter,
};

/// Wraps a type T together with an additional indicator. This way the type gains a Null
/// representation, those memory layout is compatible with ODBC.
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

unsafe impl<T> CData for Nullable<T>
where
    T: FixedSizedCType,
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

unsafe impl<T> HasDataType for Nullable<T>
where
    T: FixedSizedCType + HasDataType,
{
    fn data_type(&self) -> crate::DataType {
        self.value.data_type()
    }
}

unsafe impl<T> Parameter for Nullable<T> where T: FixedSizedCType + Parameter {}

unsafe impl<T> CDataMut for Nullable<T>
where
    T: FixedSizedCType,
{
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        &mut self.indicator as *mut isize
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        &mut self.value as *mut T as *mut c_void
    }
}

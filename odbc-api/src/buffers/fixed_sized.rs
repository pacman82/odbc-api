use crate::{fixed_sized::{Bit, FixedSizedCType}, handles::{CData, CDataMut}};
use odbc_sys::{Date, Len, Numeric, Time, Timestamp, NULL_DATA};
use std::{ffi::c_void, ptr::{null, null_mut}, mem::size_of};

pub type OptF64Column = OptFixedSizedColumn<f64>;
pub type OptF32Column = OptFixedSizedColumn<f32>;
pub type OptDateColumn = OptFixedSizedColumn<Date>;
pub type OptTimestampColumn = OptFixedSizedColumn<Timestamp>;
pub type OptTimeColumn = OptFixedSizedColumn<Time>;
pub type OptI32Column = OptFixedSizedColumn<i32>;
pub type OptI64Column = OptFixedSizedColumn<i64>;
pub type OptNumericColumn = OptFixedSizedColumn<Numeric>;
pub type OptU8Column = OptFixedSizedColumn<u8>;
pub type OptI8Column = OptFixedSizedColumn<i8>;
pub type OptBitColumn = OptFixedSizedColumn<Bit>;

/// Column buffer for fixed sized type, also binding an indicator buffer to handle NULL.
pub struct OptFixedSizedColumn<T> {
    values: Vec<T>,
    indicators: Vec<Len>,
}

impl<T> OptFixedSizedColumn<T>
where
    T: Default + Clone,
{
    pub fn new(batch_size: usize) -> Self {
        Self {
            values: vec![T::default(); batch_size],
            indicators: vec![NULL_DATA; batch_size],
        }
    }

    /// Access the value at a specific row index.
    ///
    /// # Safety
    ///
    /// The buffer size is not automatically adjusted to the size of the last row set. It is the
    /// callers responsibility to ensure, a value has been written to the indexed position by
    /// `Cursor::fetch` using the value bound to the cursor with
    /// `Cursor::set_num_result_rows_fetched`.
    pub unsafe fn value_at(&self, row_index: usize) -> Option<&T> {
        if self.indicators[row_index] == NULL_DATA {
            None
        } else {
            Some(&self.values[row_index])
        }
    }

    pub fn values(&self) -> &[T] {
        &self.values
    }

    pub fn indicators(&self) -> &[Len] {
        &self.indicators
    }
}

unsafe impl<T> CData for OptFixedSizedColumn<T>
where
    T: FixedSizedCType,
{
    fn cdata_type(&self) -> odbc_sys::CDataType {
        T::C_DATA_TYPE
    }

    fn indicator_ptr(&self) -> *const Len {
        self.indicators.as_ptr() as *const Len
    }

    fn value_ptr(&self) -> *const c_void {
        self.values.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> Len {
        size_of::<T>() as Len
    }
}

unsafe impl<T> CDataMut for OptFixedSizedColumn<T> where T: FixedSizedCType {
    fn mut_indicator_ptr(&mut self) -> *mut Len {
        self.indicators.as_mut_ptr() as *mut Len
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.values.as_mut_ptr() as *mut c_void
    }
}

unsafe impl<T> CData for Vec<T> where T: FixedSizedCType{
    fn cdata_type(&self) -> odbc_sys::CDataType {
        T::C_DATA_TYPE
    }

    fn indicator_ptr(&self) -> *const Len {
        null()
    }

    fn value_ptr(&self) -> *const c_void {
        self.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> Len {
        size_of::<T>() as Len
    }
}

unsafe impl<T> CDataMut for Vec<T> where T: FixedSizedCType {
    fn mut_indicator_ptr(&mut self) -> *mut Len {
        null_mut()
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.as_mut_ptr() as *mut c_void
    }
}
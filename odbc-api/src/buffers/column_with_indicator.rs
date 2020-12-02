use crate::{
    fixed_sized::{Bit, FixedSizedCType},
    handles::{CData, CDataMut},
};
use odbc_sys::{Date, Time, Timestamp, NULL_DATA};
use std::{
    convert::TryInto,
    ffi::c_void,
    mem::size_of,
    ptr::{null, null_mut},
};

pub type OptF64Column = ColumnWithIndicator<f64>;
pub type OptF32Column = ColumnWithIndicator<f32>;
pub type OptDateColumn = ColumnWithIndicator<Date>;
pub type OptTimestampColumn = ColumnWithIndicator<Timestamp>;
pub type OptTimeColumn = ColumnWithIndicator<Time>;
pub type OptI8Column = ColumnWithIndicator<i8>;
pub type OptI16Column = ColumnWithIndicator<i16>;
pub type OptI32Column = ColumnWithIndicator<i32>;
pub type OptI64Column = ColumnWithIndicator<i64>;
pub type OptU8Column = ColumnWithIndicator<u8>;
pub type OptBitColumn = ColumnWithIndicator<Bit>;

/// Column buffer for fixed sized type, also binding an indicator buffer to handle NULL.
#[derive(Debug)]
pub struct ColumnWithIndicator<T> {
    values: Vec<T>,
    indicators: Vec<isize>,
}

impl<T> ColumnWithIndicator<T>
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
    pub unsafe fn iter(&self, num_rows: usize) -> OptIt<'_, T> {
        OptIt {
            indicators: &self.indicators[0..num_rows],
            values: &self.values[0..num_rows],
        }
    }
}

#[derive(Debug)]
pub struct OptIt<'a, T> {
    indicators: &'a [isize],
    values: &'a [T],
}

impl<'a, T> Iterator for OptIt<'a, T> {
    type Item = Option<&'a T>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(&ind) = self.indicators.first() {
            let item = if ind == NULL_DATA {
                None
            } else {
                Some(&self.values[0])
            };
            self.indicators = &self.indicators[1..];
            self.values = &self.values[1..];
            Some(item)
        } else {
            None
        }
    }
}

unsafe impl<T> CData for ColumnWithIndicator<T>
where
    T: FixedSizedCType,
{
    fn cdata_type(&self) -> odbc_sys::CDataType {
        T::C_DATA_TYPE
    }

    fn indicator_ptr(&self) -> *const isize {
        self.indicators.as_ptr() as *const isize
    }

    fn value_ptr(&self) -> *const c_void {
        self.values.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> isize {
        size_of::<T>().try_into().unwrap()
    }
}

unsafe impl<T> CDataMut for ColumnWithIndicator<T>
where
    T: FixedSizedCType,
{
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        self.indicators.as_mut_ptr() as *mut isize
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.values.as_mut_ptr() as *mut c_void
    }
}

unsafe impl<T> CData for Vec<T>
where
    T: FixedSizedCType,
{
    fn cdata_type(&self) -> odbc_sys::CDataType {
        T::C_DATA_TYPE
    }

    fn indicator_ptr(&self) -> *const isize {
        null()
    }

    fn value_ptr(&self) -> *const c_void {
        self.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> isize {
        size_of::<T>().try_into().unwrap()
    }
}

unsafe impl<T> CDataMut for Vec<T>
where
    T: FixedSizedCType,
{
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        null_mut()
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.as_mut_ptr() as *mut c_void
    }
}

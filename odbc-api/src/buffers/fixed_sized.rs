use std::ptr::null_mut;

use super::{BindColParameters, ColumnBuffer};
use odbc_sys::{
    CDataType, Char, Date, Integer, Len, Numeric, Pointer, SChar, SmallInt, Time, Timestamp,
    UInteger, USmallInt, NULL_DATA,
};

pub type OptF64Column = OptFixedSizedColumn<f64>;
pub type OptF32Column = OptFixedSizedColumn<f32>;
pub type OptDateColumn = OptFixedSizedColumn<Date>;
pub type OptTimestampColumn = OptFixedSizedColumn<Timestamp>;
pub type OptTimeColumn = OptFixedSizedColumn<Time>;
pub type OptI32Column = OptFixedSizedColumn<i32>;
pub type OptI64Column = OptFixedSizedColumn<i64>;
pub type OptNumericColumn = OptFixedSizedColumn<Numeric>;

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

unsafe impl<T> ColumnBuffer for OptFixedSizedColumn<T>
where
    T: FixedSizedCType,
{
    fn bind_arguments(&mut self) -> BindColParameters {
        BindColParameters {
            target_type: T::C_DATA_TYPE,
            target_value: self.values.as_mut_ptr() as Pointer,
            target_length: 0,
            indicator: self.indicators.as_mut_ptr(),
        }
    }
}

unsafe impl<T> ColumnBuffer for Vec<T>
where
    T: FixedSizedCType,
{
    fn bind_arguments(&mut self) -> BindColParameters {
        BindColParameters {
            target_type: T::C_DATA_TYPE,
            target_value: self.as_mut_ptr() as Pointer,
            target_length: 0,
            indicator: null_mut(),
        }
    }
}

/// Trait implemented to fixed C size types.
pub unsafe trait FixedSizedCType: Default + Clone + Copy {
    /// ODBC C Data type used to bind instances to a statement.
    const C_DATA_TYPE: CDataType;
}

unsafe impl FixedSizedCType for f64 {
    const C_DATA_TYPE: CDataType = CDataType::Double;
}

unsafe impl FixedSizedCType for f32 {
    const C_DATA_TYPE: CDataType = CDataType::Float;
}

unsafe impl FixedSizedCType for Date {
    const C_DATA_TYPE: CDataType = CDataType::TypeDate;
}

unsafe impl FixedSizedCType for Timestamp {
    const C_DATA_TYPE: CDataType = CDataType::TypeTimestamp;
}

unsafe impl FixedSizedCType for Time {
    const C_DATA_TYPE: CDataType = CDataType::TypeTime;
}

unsafe impl FixedSizedCType for Numeric {
    const C_DATA_TYPE: CDataType = CDataType::Numeric;
}

unsafe impl FixedSizedCType for SmallInt {
    const C_DATA_TYPE: CDataType = CDataType::SShort;
}

unsafe impl FixedSizedCType for USmallInt {
    const C_DATA_TYPE: CDataType = CDataType::UShort;
}

unsafe impl FixedSizedCType for Integer {
    const C_DATA_TYPE: CDataType = CDataType::SLong;
}

unsafe impl FixedSizedCType for UInteger {
    const C_DATA_TYPE: CDataType = CDataType::ULong;
}

unsafe impl FixedSizedCType for SChar {
    const C_DATA_TYPE: CDataType = CDataType::STinyInt;
}

unsafe impl FixedSizedCType for Char {
    const C_DATA_TYPE: CDataType = CDataType::UTinyInty;
}

unsafe impl FixedSizedCType for i64 {
    const C_DATA_TYPE: CDataType = CDataType::SBigInt;
}

unsafe impl FixedSizedCType for u64 {
    const C_DATA_TYPE: CDataType = CDataType::UBigInt;
}

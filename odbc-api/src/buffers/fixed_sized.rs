use std::ptr::null_mut;

use super::{BindColArgs, ColumnBuffer};
use odbc_sys::{CDataType, Date, Len, Numeric, Pointer, Time, Timestamp, NULL_DATA};

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

/// Newtype wrapping u8 and binding as SQL_BIT.
///
/// If rust would guarantee the representation of `bool` to be an `u8`, `bool` would be the obvious
/// choice instead. Alas it is not and someday on some platform bool might be something else than a
/// `u8` so let's use this newtype instead.
#[derive(Clone, Copy, Default, PartialEq, Eq, Ord, PartialOrd)]
pub struct Bit(pub u8);

impl Bit {
    pub fn as_bool(self) -> bool {
        match self.0 {
            0 => false,
            1 => true,
            _ => panic!("Invalid boolean representation in Bit."),
        }
    }
}

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

unsafe impl<T> ColumnBuffer for OptFixedSizedColumn<T>
where
    T: FixedSizedCType,
{
    fn bind_arguments(&mut self) -> BindColArgs {
        BindColArgs {
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
    fn bind_arguments(&mut self) -> BindColArgs {
        BindColArgs {
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

unsafe impl FixedSizedCType for i16 {
    const C_DATA_TYPE: CDataType = CDataType::SShort;
}

unsafe impl FixedSizedCType for u16 {
    const C_DATA_TYPE: CDataType = CDataType::UShort;
}

unsafe impl FixedSizedCType for i32 {
    const C_DATA_TYPE: CDataType = CDataType::SLong;
}

unsafe impl FixedSizedCType for u32 {
    const C_DATA_TYPE: CDataType = CDataType::ULong;
}

unsafe impl FixedSizedCType for i8 {
    const C_DATA_TYPE: CDataType = CDataType::STinyInt;
}

unsafe impl FixedSizedCType for u8 {
    const C_DATA_TYPE: CDataType = CDataType::UTinyInty;
}

unsafe impl FixedSizedCType for Bit {
    const C_DATA_TYPE: CDataType = CDataType::Bit;
}

unsafe impl FixedSizedCType for i64 {
    const C_DATA_TYPE: CDataType = CDataType::SBigInt;
}

unsafe impl FixedSizedCType for u64 {
    const C_DATA_TYPE: CDataType = CDataType::UBigInt;
}

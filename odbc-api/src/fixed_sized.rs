use crate::{Parameter, handles::{CData, DataType, Input}};
use odbc_sys::{Len, CDataType, Date, Timestamp, Time, Numeric};
use std::{ptr::null, ffi::c_void};

/// New type wrapping u8 and binding as SQL_BIT.
///
/// If rust would guarantee the representation of `bool` to be an `u8`, `bool` would be the obvious
/// choice instead. Alas it is not and someday on some platform `bool` might be something else than
/// a `u8` so let's use this new type instead.
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


// While the C-Type is independent of the Data (SQL) Type in the source, there are often DataTypes
// which are a natural match for the C-Type in question. These can be used to spare the user to
// specify that an i32 is supposed to be bound as an SQL Integer.

macro_rules! impl_input_fixed_sized {
    ($t:ident, $data_type:expr) => {
        unsafe impl CData for $t {
            fn cdata_type(&self) -> CDataType {
                $t::C_DATA_TYPE
            }

            fn indicator_ptr(&self) -> *const Len {
                // Fixed sized types do not require a length indicator.
                null()
            }

            fn value_ptr(&self) -> *const c_void {
                self as *const $t as *const c_void
            }
        }

        unsafe impl Input for $t {
            fn data_type(&self) -> DataType {
                $data_type
            }
        }

        unsafe impl Parameter for $t {}
    };
}

impl_input_fixed_sized!(f64, DataType::Double);
impl_input_fixed_sized!(f32, DataType::Real);
impl_input_fixed_sized!(Date, DataType::Date);
impl_input_fixed_sized!(i16, DataType::SmallInt);
impl_input_fixed_sized!(i32, DataType::Integer);
impl_input_fixed_sized!(i8, DataType::Tinyint);
impl_input_fixed_sized!(Bit, DataType::Bit);
impl_input_fixed_sized!(i64, DataType::Bigint);

// Support for fixed size types, which are not unsigned. Time, Date and timestamp types could be
// supported, implementation DataType would need to take an instance into account.

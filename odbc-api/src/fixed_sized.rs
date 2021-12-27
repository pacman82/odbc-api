use crate::{
    handles::{CData, DataType, HasDataType},
    parameter::{InputParameter, StableCData},
};
use odbc_sys::{CDataType, Date, Numeric, Time, Timestamp};
use std::{ffi::c_void, ptr::null};

/// New type wrapping u8 and binding as SQL_BIT.
///
/// If rust would guarantee the representation of `bool` to be an `u8`, `bool` would be the obvious
/// choice instead. Alas it is not and someday on some platform `bool` might be something else than
/// a `u8` so let's use this new type instead.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Ord, PartialOrd)]
pub struct Bit(pub u8);

impl Bit {
    /// Maps `1` to `true`, `0` to `false`. Panics if `Bit` should be invalid (not `0` or `1`).
    pub fn as_bool(self) -> bool {
        match self.0 {
            0 => false,
            1 => true,
            _ => panic!("Invalid boolean representation in Bit."),
        }
    }
}

/// A plain old data type. With an associated C Type. Must be completely stack allocated without any
/// external references. In addition to that the buffer size must be known to ODBC in advance.
///
/// # Safety
///
/// A type implementing this trait, must be a fixed sized type. The information in the `C_DATA_TYPE`
/// constant must be enough to determine both the size and the buffer length of an Instance.
pub unsafe trait Pod: Default + Copy + CData {
    /// ODBC C Data type used to bind instances to a statement.
    const C_DATA_TYPE: CDataType;
}

macro_rules! impl_fixed_sized {
    ($t:ident, $c_data_type:expr) => {
        unsafe impl CData for $t {
            fn cdata_type(&self) -> CDataType {
                $c_data_type
            }

            fn indicator_ptr(&self) -> *const isize {
                // Fixed sized types do not require a length indicator.
                null()
            }

            fn value_ptr(&self) -> *const c_void {
                self as *const $t as *const c_void
            }

            fn buffer_length(&self) -> isize {
                0
            }
        }

        unsafe impl Pod for $t {
            const C_DATA_TYPE: CDataType = $c_data_type;
        }
    };
}

impl_fixed_sized!(f64, CDataType::Double);
impl_fixed_sized!(f32, CDataType::Float);
impl_fixed_sized!(Date, CDataType::TypeDate);
impl_fixed_sized!(Timestamp, CDataType::TypeTimestamp);
impl_fixed_sized!(Time, CDataType::TypeTime);
impl_fixed_sized!(Numeric, CDataType::Numeric);
impl_fixed_sized!(i16, CDataType::SShort);
impl_fixed_sized!(u16, CDataType::UShort);
impl_fixed_sized!(i32, CDataType::SLong);
impl_fixed_sized!(u32, CDataType::ULong);
impl_fixed_sized!(i8, CDataType::STinyInt);
impl_fixed_sized!(u8, CDataType::UTinyInt);
impl_fixed_sized!(Bit, CDataType::Bit);
impl_fixed_sized!(i64, CDataType::SBigInt);
impl_fixed_sized!(u64, CDataType::UBigInt);

// While the C-Type is independent of the Data (SQL) Type in the source, there are often DataTypes
// which are a natural match for the C-Type in question. These can be used to spare the user to
// specify that an i32 is supposed to be bound as an SQL Integer.

macro_rules! impl_input_fixed_sized {
    ($t:ident, $data_type:expr) => {
        impl HasDataType for $t {
            fn data_type(&self) -> DataType {
                $data_type
            }
        }

        unsafe impl InputParameter for $t {}

        unsafe impl StableCData for $t {}
    };
}

impl_input_fixed_sized!(f64, DataType::Double);
impl_input_fixed_sized!(f32, DataType::Real);
impl_input_fixed_sized!(Date, DataType::Date);
impl_input_fixed_sized!(i16, DataType::SmallInt);
impl_input_fixed_sized!(i32, DataType::Integer);
impl_input_fixed_sized!(i8, DataType::TinyInt);
impl_input_fixed_sized!(Bit, DataType::Bit);
impl_input_fixed_sized!(i64, DataType::BigInt);

// Support for fixed size types, which are not unsigned. Time, Date and timestamp types could be
// supported, implementation DataType would need to take an instance into account.

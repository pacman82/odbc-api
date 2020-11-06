use crate::buffers::Bit;
use crate::DataType;
use odbc_sys::Date;

/// While the C-Type is independent of the Data (SQL) Type in the source, there are often DataTypes
/// which are a natural match for the C-Type in question. These can be used to spare the user to
/// specify that an i32 is supposed to be bound as an SQL Integer.
pub trait DefaultDataType {
    /// The data type used then binding an instance as an argument in absence of other information.
    const DEFAULT_DATA_TYPE: DataType;
}

// Support for fixed size types, which are not unsigned. Time, Date and timestamp types could be
// supported, if trait switched from Constant to a method taking the instance into account.

impl DefaultDataType for f64 {
    const DEFAULT_DATA_TYPE: DataType = DataType::Double;
}

impl DefaultDataType for f32 {
    const DEFAULT_DATA_TYPE: DataType = DataType::Real;
}

impl DefaultDataType for Date {
    const DEFAULT_DATA_TYPE: DataType = DataType::Date;
}

impl DefaultDataType for i16 {
    const DEFAULT_DATA_TYPE: DataType = DataType::SmallInt;
}

impl DefaultDataType for i32 {
    const DEFAULT_DATA_TYPE: DataType = DataType::Integer;
}

impl DefaultDataType for i8 {
    const DEFAULT_DATA_TYPE: DataType = DataType::Tinyint;
}

impl DefaultDataType for Bit {
    const DEFAULT_DATA_TYPE: DataType = DataType::Bit;
}

impl DefaultDataType for i64 {
    const DEFAULT_DATA_TYPE: DataType = DataType::Bigint;
}

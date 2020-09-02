use odbc_sys::{Date, CDataType, Timestamp, SmallInt, USmallInt, Integer, UInteger, Char, SChar};

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

use crate::DataType;

/// Used to describe a column of a [`crate::buffers::ColumnarRowSet`].
///
/// While related to to the [`crate::DataType`] of the column this is bound to, the Buffer type is
/// different as it does not describe the type of the data source but the format the data is going
/// to be represented in memory. While the data source is often considered to choose the buffer type
/// the kind of processing which is supposed to be applied to the data may be even more important
/// if choosing the a buffer for the cursor type. I.e. if you intend to print a date to standard out
/// it may be more reasonable to bind it as `Text` rather than `Date`.
#[derive(Clone, Copy, Debug)]
pub struct BufferDescription {
    /// This indicates whether or not the buffer will be able to represent NULL values. This will
    /// cause an indicator buffer to be bound if the selected buffer kind does not already require
    /// one anyway.
    pub nullable: bool,
    /// The type of CData the buffer will be holding.
    pub kind: BufferKind,
}

/// This class is used together with [`crate::buffers::BufferDescription`] to specify the layout of
/// buffers bound to ODBC cursors and statements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BufferKind {
    /// Text buffer holding strings with binary length of up to `max_str_len`.
    Text {
        /// Maximum string length. Terminating zero is excluded, i.e. memory for it will be
        /// implicitly allocated if required.
        max_str_len: usize
    },
    /// 64 bit floating point
    F64,
    /// 32 bit floating point
    F32,
    /// Describes a buffer holding [`crate::sys::Date`] values.
    Date,
    /// Describes a buffer holding [`crate::sys::Time`] values.
    Time,
    /// Describes a buffer holding [`crate::sys::Timestamp`] values.
    Timestamp,
    /// Signed 8 Bit integer
    I8,
    /// Signed 16 Bit integer
    I16,
    /// Signed 32 Bit integer
    I32,
    /// Signed 64 Bit integer
    I64,
    /// Unsigned 8 Bit integer
    U8,
    /// Can either be zero or one
    Bit,
}

impl BufferKind {
    /// Describe a buffer which fits best the SQL Data Type.
    ///
    /// ```
    /// use odbc_api::{DataType, buffers::BufferKind};
    ///
    /// assert_eq!(BufferKind::from_data_type(DataType::Unknown), None);
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Numeric { precision: 2, scale: 0 }),
    ///     Some(BufferKind::I8)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Numeric { precision: 9, scale: 0 }),
    ///     Some(BufferKind::I32)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Numeric { precision: 18, scale: 0 }),
    ///     Some(BufferKind::I64)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Numeric { precision: 20, scale: 5 }),
    ///     Some(BufferKind::Text { max_str_len: 20 + 2 })
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Varchar { length: 42 }),
    ///     Some(BufferKind::Text { max_str_len: 42 })
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::BigInt),
    ///     Some(BufferKind::I64)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Integer),
    ///     Some(BufferKind::I32)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::SmallInt),
    ///     Some(BufferKind::I16)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::TinyInt),
    ///     Some(BufferKind::I8)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Float),
    ///     Some(BufferKind::F32)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Double),
    ///     Some(BufferKind::F64)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Date),
    ///     Some(BufferKind::Date)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Time { precision: 0 }),
    ///     Some(BufferKind::Time)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Bit),
    ///     Some(BufferKind::Bit)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Time { precision: 3 }),
    ///     Some(BufferKind::Text { max_str_len: 12 })
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Timestamp { precision: 3 }),
    ///     Some(BufferKind::Timestamp)
    /// );
    /// ```
    pub fn from_data_type(data_type: DataType) -> Option<Self> {
        let buffer_kind = match data_type {
            DataType::Unknown
            | DataType::Other { data_type: _, column_size: _, decimal_digits: _ } => return None,
            DataType::Numeric { precision, scale }
            | DataType::Decimal { precision, scale } if scale == 0 && precision < 3 => BufferKind::I8,
            DataType::Numeric { precision, scale }
            | DataType::Decimal { precision, scale } if scale == 0 && precision < 10 => BufferKind::I32,
            DataType::Numeric { precision, scale }
            | DataType::Decimal { precision, scale } if scale == 0 && precision < 19 => BufferKind::I64,
            DataType::Integer => BufferKind::I32,
            DataType::SmallInt => BufferKind::I16,
            DataType::Float | DataType::Real => BufferKind::F32,
            DataType::Double => BufferKind::F64,
            DataType::Date => BufferKind::Date,
            DataType::Time { precision: 0 } => BufferKind::Time,
            DataType::Timestamp { precision: _ } => BufferKind::Timestamp,
            DataType::BigInt => BufferKind::I64,
            DataType::TinyInt => BufferKind::I8,
            DataType::Bit => BufferKind::Bit,
            DataType::Varchar { length }
            | DataType::WVarchar { length }
            // Currently no special buffers for fixed lengths text implemented.
            | DataType::Char { length } => BufferKind::Text { max_str_len : length },
            // Specialized buffers for Numeric and decimal are not yet supported.
            | DataType::Numeric { precision: _, scale: _ }
            | DataType::Decimal { precision: _, scale: _ }
            | DataType::Time { precision: _ } => BufferKind::Text { max_str_len: data_type.display_size().unwrap() },
        };
        Some(buffer_kind)
    }
}

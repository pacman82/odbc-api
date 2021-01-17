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
#[derive(Clone, Copy, Debug)]
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
    /// Describes a buffer holding [`sys::Date`] values.
    Date,
    /// Describes a buffer holding [`sys::Time`] values.
    Time,
    /// Describes a buffer holding [`sys::Timestamp`] values.
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
            DataType::Varchar { length }
            | DataType::WVarchar { length }
            // Currently no special buffers for fixed lengths text implemented.
            | DataType::Char { length } => BufferKind::Text { max_str_len : length },
            // Specialized buffers for Numeric and decimal are not yet supported.
            | DataType::Numeric { precision: _, scale: _ }
            | DataType::Decimal { precision: _, scale: _ } => BufferKind::Text { max_str_len: data_type.column_size() },
            DataType::Integer => BufferKind::I32,
            DataType::SmallInt => BufferKind::I16,
            DataType::Float => BufferKind::F32,
            DataType::Real => BufferKind::F32,
            DataType::Double => BufferKind::F64,
            DataType::Date => BufferKind::Date,
            DataType::Time { precision: _ } => BufferKind::Time,
            DataType::Timestamp { precision: _ } => BufferKind::Timestamp,
            DataType::Bigint => BufferKind::I64,
            DataType::Tinyint => BufferKind::I8,
            DataType::Bit => BufferKind::Bit,
        };
        Some(buffer_kind)
    }
}

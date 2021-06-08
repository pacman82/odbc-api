use super::data_type::DataType;
use std::char::{decode_utf16, DecodeUtf16Error};

/// Indication of whether a column is nullable or not.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Nullability {
    /// Indicates that we do not know whether the column is Nullable or not.
    Unknown,
    /// The column may hold NULL values.
    Nullable,
    /// The column can not hold NULL values.
    NoNulls,
}

impl Default for Nullability {
    fn default() -> Self {
        Nullability::Unknown
    }
}

impl Nullability {
    /// Construct a new instance from a `Nullability` new type constant.
    pub fn new(nullability: odbc_sys::Nullability) -> Self {
        match nullability {
            odbc_sys::Nullability::UNKNOWN => Nullability::Unknown,
            odbc_sys::Nullability::NO_NULLS => Nullability::NoNulls,
            odbc_sys::Nullability::NULLABLE => Nullability::Nullable,
            other => panic!("ODBC returned invalid value for Nullable: {:?}", other),
        }
    }
}

/// Describes the type and attributes of a column.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ColumnDescription {
    /// Column name. May be empty if unavailable.
    pub name: Vec<u16>,
    /// Type of the column
    pub data_type: DataType,
    /// Indicates whether the column is nullable or not.
    pub nullability: Nullability,
}

impl ColumnDescription {
    /// Converts the internal UTF16 representation of the column name into UTF8 and returns the
    /// result as a `String`.
    pub fn name_to_string(&self) -> Result<String, DecodeUtf16Error> {
        decode_utf16(self.name.iter().copied()).collect()
    }

    /// `true` if the column is `Nullable` or it is not know whether the column is nullable. `false`
    /// if and only if the column is `NoNulls`.
    pub fn could_be_nullable(&self) -> bool {
        match self.nullability {
            Nullability::Nullable | Nullability::Unknown => true,
            Nullability::NoNulls => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ExtendedColumnDescription {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub column_name: String,
    pub data_type: DataType,
    pub type_name: String,
    pub buffer_length: Option<i32>,
    pub precision_radix: Option<i16>,
    pub nullability: Nullability,
    pub remarks: String,
    pub column_default: String,
    pub char_octet_length: Option<i32>,
    pub ordinal_position: i32,
}

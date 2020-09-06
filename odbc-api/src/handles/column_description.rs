use odbc_sys::{SmallInt, SqlDataType, ULen, WChar};
use std::char::{decode_utf16, DecodeUtf16Error};

/// Indication of wether a column is nullable or not.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Nullable {
    Unknown,
    Nullable,
    NoNulls,
}

/// Describes the type and attributes of a column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnDescription {
    /// Colmun name. May be empty if unavailable.
    pub name: Vec<WChar>,
    /// Type of the column
    pub data_type: SqlDataType,
    /// Size of column element
    pub column_size: ULen,
    pub decimal_digits: SmallInt,
    /// Indicates wether the column is nullable or not.
    pub nullable: Nullable,
}

impl Default for ColumnDescription {
    fn default() -> Self {
        Self {
            name: Vec::new(),
            data_type: SqlDataType::UNKNOWN_TYPE,
            column_size: 0,
            decimal_digits: 0,
            nullable: Nullable::Unknown,
        }
    }
}

impl ColumnDescription {
    /// Converts the internal UTF16 representation of the column name into UTF8 and returns the
    /// result as a `String`.
    pub fn name_to_string(&self) -> Result<String, DecodeUtf16Error> {
        decode_utf16(self.name.iter().copied()).collect()
    }

    /// `true` if the column is `Nullable` or it is not know wether the column is nullable. `false`
    /// if and only if the column is `NoNulls`.
    pub fn could_be_nullable(&self) -> bool {
        match self.nullable {
            Nullable::Nullable | Nullable::Unknown => true,
            Nullable::NoNulls => false,
        }
    }
}

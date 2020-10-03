use super::data_type::DataType;
use odbc_sys::WChar;
use std::char::{decode_utf16, DecodeUtf16Error};

/// Indication of whether a column is nullable or not.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Nullable {
    Unknown,
    Nullable,
    NoNulls,
}

impl Default for Nullable {
    fn default() -> Self {
        Nullable::Unknown
    }
}

/// Describes the type and attributes of a column.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ColumnDescription {
    /// Column name. May be empty if unavailable.
    pub name: Vec<WChar>,
    /// Type of the column
    pub data_type: DataType,
    /// Indicates whether the column is nullable or not.
    pub nullable: Nullable,
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
        match self.nullable {
            Nullable::Nullable | Nullable::Unknown => true,
            Nullable::NoNulls => false,
        }
    }
}

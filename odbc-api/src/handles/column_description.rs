use super::data_type::DataType;
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

impl Nullable {
    pub fn new(nullable: odbc_sys::Nullable) -> Self {
        match nullable {
            odbc_sys::Nullable::UNKNOWN => Nullable::Unknown,
            odbc_sys::Nullable::NO_NULLS => Nullable::NoNulls,
            odbc_sys::Nullable::NULLABLE => Nullable::Nullable,
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

use odbc_sys::{Nullable, SqlDataType, WChar, SmallInt, ULen};

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
        Self{
            name: Vec::new(),
            data_type: SqlDataType::UnknownType,
            column_size: 0,
            decimal_digits: 0,
            nullable: Nullable::UNKNOWN,
        }
    }
}
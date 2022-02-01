use super::{
    data_type::DataType,
    sql_char::{slice_to_utf8, DecodingError, SqlChar},
};

/// Indication of whether a column is nullable or not.
#[derive(Clone, Copy, Hash, Debug, Eq, PartialEq)]
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
    pub name: Vec<SqlChar>,
    /// Type of the column
    pub data_type: DataType,
    /// Indicates whether the column is nullable or not.
    pub nullability: Nullability,
}

impl ColumnDescription {
    /// Converts the internal UTF16 representation of the column name into UTF8 and returns the
    /// result as a `String`.
    pub fn name_to_string(&self) -> Result<String, DecodingError> {
        slice_to_utf8(&self.name)
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

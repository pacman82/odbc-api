use super::{
    data_type::DataType,
    sql_char::{slice_to_utf8, DecodingError, SqlChar},
};

/// Indication of whether a column is nullable or not.
#[derive(Clone, Copy, Hash, Debug, Eq, PartialEq, Default)]
pub enum Nullability {
    /// Indicates that we do not know whether the column is Nullable or not.
    #[default]
    Unknown,
    /// The column may hold NULL values.
    Nullable,
    /// The column can not hold NULL values.
    NoNulls,
}

impl Nullability {
    /// Construct a new instance from a `Nullability` new type constant.
    ///
    /// ```
    /// use odbc_api::Nullability;
    ///
    /// assert_eq!(Nullability::Unknown, Nullability::new(odbc_sys::Nullability::UNKNOWN));
    /// assert_eq!(Nullability::NoNulls, Nullability::new(odbc_sys::Nullability::NO_NULLS));
    /// assert_eq!(Nullability::Nullable, Nullability::new(odbc_sys::Nullability::NULLABLE));
    /// ```
    pub fn new(nullability: odbc_sys::Nullability) -> Self {
        match nullability {
            odbc_sys::Nullability::UNKNOWN => Nullability::Unknown,
            odbc_sys::Nullability::NO_NULLS => Nullability::NoNulls,
            odbc_sys::Nullability::NULLABLE => Nullability::Nullable,
            other => panic!("ODBC returned invalid value for Nullable: {}", other.0),
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
    /// In production, an 'empty' [`ColumnDescription`] is expected to be constructed via the
    /// [`Default`] trait. It is then filled using [`crate::ResultSetMetadata::describe_col`]. When
    /// writing test cases however it might be desirable to directly instantiate a
    /// [`ColumnDescription`]. This constructor enables you to do that, without caring which type
    /// `SqlChar` resolves to.
    pub fn new(name: &str, data_type: DataType, nullability: Nullability) -> Self {
        #[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
        pub fn utf8_to_vec_char(text: &str) -> Vec<u8> {
            text.to_owned().into_bytes()
        }
        #[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
        pub fn utf8_to_vec_char(text: &str) -> Vec<u16> {
            use widestring::U16String;
            U16String::from_str(text).into_vec()
        }
        Self {
            name: utf8_to_vec_char(name),
            data_type,
            nullability,
        }
    }

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

#[cfg(test)]
mod tests {
    use crate::Nullability;

    /// Application should panic if ODBC driver returns unsupported value for nullable
    #[test]
    #[should_panic(expected = "ODBC returned invalid value for Nullable: 5")]
    fn invalid_nullable_representation() {
        Nullability::new(odbc_sys::Nullability(5));
    }
}

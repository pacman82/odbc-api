use crate::{
    CursorImpl, Error, Nullable, TruncationInfo,
    buffers::{FetchRow, FetchRowMember as _},
    handles::{SqlText, Statement, StatementRef},
    parameter::VarCharArray,
};

/// A row returned by [`crate::Preallocated::columns`]. The members are associated with the
/// columns of the result set returned by [`crate::Preallocated::columns_cursor`].
///
/// Sensible defaults for the buffer sizes for the variable-length columns have been chosen.
/// However, your driver could return larger strings or be guaranteed to return shoter ones. Your
/// ODBC driver may also choose to add additional columns at the end of the result set. If you
/// want to change the buffer sizes or fetch the additional driver specific data, use [
/// [`crate::Preallocated::columns_cursor`] directly and manually bind a columnar buffer to it.
///
/// See: <https://learn.microsoft.com/sql/odbc/reference/syntax/sqlcolumns-function>
#[derive(Clone, Copy, Default)]
pub struct ColumnsRow {
    /// Binds to the `TABLE_CAT` column. Catalog name of the table. NULL if not applicable.
    pub catalog: VarCharArray<128>,
    /// Binds to the `TABLE_SCHEM` column. Schema name of the table. NULL if not applicable.
    pub schema: VarCharArray<128>,
    /// Binds to the `TABLE_NAME` column. Table name.
    pub table: VarCharArray<255>,
    /// Binds to the `COLUMN_NAME` column. Column name. Empty string for unnamed columns.
    pub column_name: VarCharArray<255>,
    /// Binds to the `DATA_TYPE` column. SQL data type.
    pub data_type: i16,
    /// Binds to the `TYPE_NAME` column. Data source-dependent data type name.
    pub type_name: VarCharArray<256>,
    /// Binds to the `COLUMN_SIZE` column. Size of the column in the data source. NULL if not
    /// applicable.
    pub column_size: Nullable<i32>,
    /// Binds to the `BUFFER_LENGTH` column. Length in bytes of data transferred on fetch.
    pub buffer_length: Nullable<i32>,
    /// Binds to the `DECIMAL_DIGITS` column. Decimal digits of the column. NULL if not applicable.
    pub decimal_digits: Nullable<i16>,
    /// Binds to the `NUM_PREC_RADIX` column. Either 10, 2, or NULL.
    pub num_prec_radix: Nullable<i16>,
    /// Binds to the `NULLABLE` column. Whether the column accepts NULLs.
    pub nullable: i16,
    /// Binds to the `REMARKS` column. Description of the column. NULL if unavailable.
    pub remarks: VarCharArray<1024>,
    /// Binds to the `COLUMN_DEF` column. Default value of the column. NULL if unspecified.
    pub column_default: VarCharArray<4000>,
    /// Binds to the `SQL_DATA_TYPE` column. SQL data type as it appears in the `SQL_DESC_TYPE`
    /// record field in the IRD.
    pub sql_data_type: i16,
    /// Binds to the `SQL_DATETIME_SUB` column. Subtype code for datetime and interval data types.
    /// NULL for other data types.
    pub sql_datetime_sub: Nullable<i16>,
    /// Binds to the `CHAR_OCTET_LENGTH` column. Maximum length in bytes of a character or binary
    /// column. NULL for other data types.
    pub char_octet_length: Nullable<i32>,
    /// Binds to the `ORDINAL_POSITION` column. Ordinal position of the column in the table
    /// (starting with 1).
    pub ordinal_position: i32,
    /// Binds to the `IS_NULLABLE` column. `"NO"` if the column is known to be not nullable.
    /// `"YES"` if the column might be nullable. Empty string if nullability is unknown.
    pub is_nullable: VarCharArray<4>,
}

unsafe impl FetchRow for ColumnsRow {
    unsafe fn bind_columns_to_cursor(&mut self, mut cursor: StatementRef<'_>) -> Result<(), Error> {
        unsafe {
            self.catalog.bind_to_col(1, &mut cursor)?;
            self.schema.bind_to_col(2, &mut cursor)?;
            self.table.bind_to_col(3, &mut cursor)?;
            self.column_name.bind_to_col(4, &mut cursor)?;
            self.data_type.bind_to_col(5, &mut cursor)?;
            self.type_name.bind_to_col(6, &mut cursor)?;
            self.column_size.bind_to_col(7, &mut cursor)?;
            self.buffer_length.bind_to_col(8, &mut cursor)?;
            self.decimal_digits.bind_to_col(9, &mut cursor)?;
            self.num_prec_radix.bind_to_col(10, &mut cursor)?;
            self.nullable.bind_to_col(11, &mut cursor)?;
            self.remarks.bind_to_col(12, &mut cursor)?;
            self.column_default.bind_to_col(13, &mut cursor)?;
            self.sql_data_type.bind_to_col(14, &mut cursor)?;
            self.sql_datetime_sub.bind_to_col(15, &mut cursor)?;
            self.char_octet_length.bind_to_col(16, &mut cursor)?;
            self.ordinal_position.bind_to_col(17, &mut cursor)?;
            self.is_nullable.bind_to_col(18, &mut cursor)?;
            Ok(())
        }
    }

    fn find_truncation(&self) -> Option<TruncationInfo> {
        if let Some(t) = self.catalog.find_truncation(0) {
            return Some(t);
        }
        if let Some(t) = self.schema.find_truncation(1) {
            return Some(t);
        }
        if let Some(t) = self.table.find_truncation(2) {
            return Some(t);
        }
        if let Some(t) = self.column_name.find_truncation(3) {
            return Some(t);
        }
        if let Some(t) = self.data_type.find_truncation(4) {
            return Some(t);
        }
        if let Some(t) = self.type_name.find_truncation(5) {
            return Some(t);
        }
        if let Some(t) = self.column_size.find_truncation(6) {
            return Some(t);
        }
        if let Some(t) = self.buffer_length.find_truncation(7) {
            return Some(t);
        }
        if let Some(t) = self.decimal_digits.find_truncation(8) {
            return Some(t);
        }
        if let Some(t) = self.num_prec_radix.find_truncation(9) {
            return Some(t);
        }
        if let Some(t) = self.nullable.find_truncation(10) {
            return Some(t);
        }
        if let Some(t) = self.remarks.find_truncation(11) {
            return Some(t);
        }
        if let Some(t) = self.column_default.find_truncation(12) {
            return Some(t);
        }
        if let Some(t) = self.sql_data_type.find_truncation(13) {
            return Some(t);
        }
        if let Some(t) = self.sql_datetime_sub.find_truncation(14) {
            return Some(t);
        }
        if let Some(t) = self.char_octet_length.find_truncation(15) {
            return Some(t);
        }
        if let Some(t) = self.ordinal_position.find_truncation(16) {
            return Some(t);
        }
        if let Some(t) = self.is_nullable.find_truncation(17) {
            return Some(t);
        }
        None
    }
}

pub fn execute_columns<S>(
    mut statement: S,
    catalog_name: &SqlText,
    schema_name: &SqlText,
    table_name: &SqlText,
    column_name: &SqlText,
) -> Result<CursorImpl<S>, Error>
where
    S: Statement,
{
    statement
        .columns(catalog_name, schema_name, table_name, column_name)
        .into_result(&statement)?;

    // We assume columns always creates a result set, since it works like a SELECT statement.
    debug_assert_ne!(statement.num_result_cols().unwrap(), 0);

    // Safe: `statement` is in cursor state
    let cursor = unsafe { CursorImpl::new(statement) };
    Ok(cursor)
}

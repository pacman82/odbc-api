use crate::{
    CursorImpl, Error, TruncationInfo,
    buffers::{FetchRow, FetchRowMember as _},
    handles::{SqlText, Statement, StatementRef},
    parameter::VarCharArray,
};

/// A row returned by the iterator returned by [`crate::Preallocated::primary_keys`]. The members
/// are associated with the columns of the result set returned by
/// [`crate::Preallocated::primary_keys_cursor`].
///
/// See: <https://learn.microsoft.com/sql/odbc/reference/syntax/sqlprimarykeys-function>
#[derive(Clone, Copy, Default)]
pub struct PrimaryKeysRow {
    // The maximum length of the VARCHAR columns is driver specific. Since this type needs to be
    // `Copy` in order to be suitable for row wise bulk fetchings sensible upper bounds have been
    // chosen. They have been determined by using the maximum lengths reported by PostgreSQL and
    // Microsoft SQL Server, MariaDB and SQLite.
    //
    /// Binds to the `TABLE_CAT` column. Primary key table catalog name. NULL if not applicable to
    /// the data source. If a driver supports catalogs for some tables but not for others, such as
    /// when the driver retrieves data from different DBMSs, it returns an empty string ("") for
    /// those tables that do not have catalogs.
    pub catalog: VarCharArray<128>,
    /// Binds to `TABLE_SCHEM`. Primary key table schema name; NULL if not applicable to the data
    /// source. If a driver supports schemas for some tables but not for others, such as when the
    /// driver retrieves data from different DBMSs, it returns an empty string ("") for those tables
    /// that do not have schemas.
    pub schema: VarCharArray<128>,
    /// Binds to the `TABLE_NAME` column. Primary key table name. Drivers must not return NULL.
    pub table: VarCharArray<255>,
    /// Binds to the `COLUMN_NAME` column. Primary key column name. The driver returns an empty
    /// string for a column that does not have a name. Drivers must not return NULL.
    pub column: VarCharArray<255>,
    /// Binds to the `KEY_SEQ` column. Column sequence number in key (starting with 1).
    pub key_seq: i16,
    /// Binds to the `PK_NAME` column. Primary key name. NULL if not applicable to the data source.
    pub pk_name: VarCharArray<128>,
}

unsafe impl FetchRow for PrimaryKeysRow {
    unsafe fn bind_columns_to_cursor(&mut self, mut cursor: StatementRef<'_>) -> Result<(), Error> {
        unsafe {
            self.catalog.bind_to_col(1, &mut cursor)?;
            self.schema.bind_to_col(2, &mut cursor)?;
            self.table.bind_to_col(3, &mut cursor)?;
            self.column.bind_to_col(4, &mut cursor)?;
            self.key_seq.bind_to_col(5, &mut cursor)?;
            self.pk_name.bind_to_col(6, &mut cursor)?;
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
        if let Some(t) = self.column.find_truncation(3) {
            return Some(t);
        }
        if let Some(t) = self.key_seq.find_truncation(4) {
            return Some(t);
        }
        if let Some(t) = self.pk_name.find_truncation(5) {
            return Some(t);
        }
        None
    }
}

pub fn execute_primary_keys<S>(
    mut statement: S,
    catalog_name: Option<&str>,
    schema_name: Option<&str>,
    table_name: &str,
) -> Result<CursorImpl<S>, Error>
where
    S: Statement,
{
    statement
        .primary_keys(
            catalog_name.map(SqlText::new).as_ref(),
            schema_name.map(SqlText::new).as_ref(),
            &SqlText::new(table_name),
        )
        .into_result(&statement)?;
    // SAFETY: primary_keys puts stmt into cursor state.
    let cursor = unsafe { CursorImpl::new(statement) };
    Ok(cursor)
}

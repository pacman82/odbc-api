use crate::{
    CursorImpl, Error, Nullable, TruncationInfo,
    buffers::{FetchRow, FetchRowMember as _},
    handles::{SqlText, Statement, StatementRef},
    parameter::VarCharArray,
};

/// A row returned by [`crate::Preallocated::foreign_keys`]. The members are associated with the
/// columns of the result set returned by [`crate::Preallocated::foreign_keys_cursor`].
///
/// See: <https://learn.microsoft.com/sql/odbc/reference/syntax/sqlforeignkeys-function>
#[derive(Clone, Copy, Default)]
pub struct ForeignKeysRow {
    /// Binds to the `PKTABLE_CAT` column. Primary key table catalog name. NULL if not applicable.
    pub pk_catalog: VarCharArray<128>,
    /// Binds to the `PKTABLE_SCHEM` column. Primary key table schema name. NULL if not applicable.
    pub pk_schema: VarCharArray<128>,
    /// Binds to the `PKTABLE_NAME` column. Primary key table name.
    pub pk_table: VarCharArray<255>,
    /// Binds to the `PKCOLUMN_NAME` column. Primary key column name.
    pub pk_column: VarCharArray<255>,
    /// Binds to the `FKTABLE_CAT` column. Foreign key table catalog name. NULL if not applicable.
    pub fk_catalog: VarCharArray<128>,
    /// Binds to the `FKTABLE_SCHEM` column. Foreign key table schema name. NULL if not applicable.
    pub fk_schema: VarCharArray<128>,
    /// Binds to the `FKTABLE_NAME` column. Foreign key table name.
    pub fk_table: VarCharArray<255>,
    /// Binds to the `FKCOLUMN_NAME` column. Foreign key column name.
    pub fk_column: VarCharArray<255>,
    /// Binds to the `KEY_SEQ` column. Column sequence number in key (starting with 1).
    pub key_seq: i16,
    /// Binds to the `UPDATE_RULE` column. Action applied to the foreign key when the SQL operation
    /// is UPDATE.
    pub update_rule: Nullable<i16>,
    /// Binds to the `DELETE_RULE` column. Action applied to the foreign key when the SQL operation
    /// is DELETE.
    pub delete_rule: Nullable<i16>,
    /// Binds to the `FK_NAME` column. Foreign key name. NULL if not applicable.
    pub fk_name: VarCharArray<128>,
    /// Binds to the `PK_NAME` column. Primary key name. NULL if not applicable.
    pub pk_name: VarCharArray<128>,
    /// Binds to the `DEFERRABILITY` column. One of: `SQL_INITIALLY_DEFERRED`,
    /// `SQL_INITIALLY_IMMEDIATE`, `SQL_NOT_DEFERRABLE`.
    pub deferrability: Nullable<i16>,
}

unsafe impl FetchRow for ForeignKeysRow {
    unsafe fn bind_columns_to_cursor(&mut self, mut cursor: StatementRef<'_>) -> Result<(), Error> {
        unsafe {
            self.pk_catalog.bind_to_col(1, &mut cursor)?;
            self.pk_schema.bind_to_col(2, &mut cursor)?;
            self.pk_table.bind_to_col(3, &mut cursor)?;
            self.pk_column.bind_to_col(4, &mut cursor)?;
            self.fk_catalog.bind_to_col(5, &mut cursor)?;
            self.fk_schema.bind_to_col(6, &mut cursor)?;
            self.fk_table.bind_to_col(7, &mut cursor)?;
            self.fk_column.bind_to_col(8, &mut cursor)?;
            self.key_seq.bind_to_col(9, &mut cursor)?;
            self.update_rule.bind_to_col(10, &mut cursor)?;
            self.delete_rule.bind_to_col(11, &mut cursor)?;
            self.fk_name.bind_to_col(12, &mut cursor)?;
            self.pk_name.bind_to_col(13, &mut cursor)?;
            self.deferrability.bind_to_col(14, &mut cursor)?;
            Ok(())
        }
    }

    fn find_truncation(&self) -> Option<TruncationInfo> {
        if let Some(t) = self.pk_catalog.find_truncation(0) {
            return Some(t);
        }
        if let Some(t) = self.pk_schema.find_truncation(1) {
            return Some(t);
        }
        if let Some(t) = self.pk_table.find_truncation(2) {
            return Some(t);
        }
        if let Some(t) = self.pk_column.find_truncation(3) {
            return Some(t);
        }
        if let Some(t) = self.fk_catalog.find_truncation(4) {
            return Some(t);
        }
        if let Some(t) = self.fk_schema.find_truncation(5) {
            return Some(t);
        }
        if let Some(t) = self.fk_table.find_truncation(6) {
            return Some(t);
        }
        if let Some(t) = self.fk_column.find_truncation(7) {
            return Some(t);
        }
        if let Some(t) = self.key_seq.find_truncation(8) {
            return Some(t);
        }
        if let Some(t) = self.update_rule.find_truncation(9) {
            return Some(t);
        }
        if let Some(t) = self.delete_rule.find_truncation(10) {
            return Some(t);
        }
        if let Some(t) = self.fk_name.find_truncation(11) {
            return Some(t);
        }
        if let Some(t) = self.pk_name.find_truncation(12) {
            return Some(t);
        }
        if let Some(t) = self.deferrability.find_truncation(13) {
            return Some(t);
        }
        None
    }
}

pub fn execute_foreign_keys<S>(
    mut statement: S,
    pk_catalog_name: &str,
    pk_schema_name: &str,
    pk_table_name: &str,
    fk_catalog_name: &str,
    fk_schema_name: &str,
    fk_table_name: &str,
) -> Result<CursorImpl<S>, Error>
where
    S: Statement,
{
    statement
        .foreign_keys(
            &SqlText::new(pk_catalog_name),
            &SqlText::new(pk_schema_name),
            &SqlText::new(pk_table_name),
            &SqlText::new(fk_catalog_name),
            &SqlText::new(fk_schema_name),
            &SqlText::new(fk_table_name),
        )
        .into_result(&statement)?;

    // We assume foreign keys always creates a result set, since it works like a SELECT statement.
    debug_assert_ne!(statement.num_result_cols().unwrap(), 0);

    // Safe: `statement` is in Cursor state.
    let cursor = unsafe { CursorImpl::new(statement) };

    Ok(cursor)
}

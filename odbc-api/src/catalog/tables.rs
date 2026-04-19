use crate::{
    CursorImpl, Error, TruncationInfo,
    buffers::{FetchRow, FetchRowMember as _},
    handles::{SqlText, Statement, StatementRef},
    parameter::VarCharArray,
};

/// A row returned by [`crate::Preallocated::tables`]. The members are associated with the columns
/// of the result set returned by [`crate::Preallocated::tables_cursor`].
///
/// See: <https://learn.microsoft.com/sql/odbc/reference/syntax/sqltables-function>
#[derive(Clone, Copy, Default)]
pub struct TablesRow {
    /// Binds to the `TABLE_CAT` column. Catalog name. NULL if not applicable.
    pub catalog: VarCharArray<128>,
    /// Binds to the `TABLE_SCHEM` column. Schema name. NULL if not applicable.
    pub schema: VarCharArray<128>,
    /// Binds to the `TABLE_NAME` column. Table name.
    pub table: VarCharArray<255>,
    /// Binds to the `TABLE_TYPE` column. Table type identifier, e.g. `"TABLE"`, `"VIEW"`.
    pub table_type: VarCharArray<255>,
    /// Binds to the `REMARKS` column. Description of the table. NULL if unavailable.
    pub remarks: VarCharArray<1024>,
}

unsafe impl FetchRow for TablesRow {
    unsafe fn bind_columns_to_cursor(&mut self, mut cursor: StatementRef<'_>) -> Result<(), Error> {
        unsafe {
            self.catalog.bind_to_col(1, &mut cursor)?;
            self.schema.bind_to_col(2, &mut cursor)?;
            self.table.bind_to_col(3, &mut cursor)?;
            self.table_type.bind_to_col(4, &mut cursor)?;
            self.remarks.bind_to_col(5, &mut cursor)?;
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
        if let Some(t) = self.table_type.find_truncation(3) {
            return Some(t);
        }
        if let Some(t) = self.remarks.find_truncation(4) {
            return Some(t);
        }
        None
    }
}

pub fn execute_tables<S>(
    mut statement: S,
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    table_type: &str,
) -> Result<CursorImpl<S>, Error>
where
    S: Statement,
{
    statement
        .tables(
            &SqlText::new(catalog_name),
            &SqlText::new(schema_name),
            &SqlText::new(table_name),
            &SqlText::new(table_type),
        )
        .into_result(&statement)?;

    // We assume tables always creates a result set, since it works like a SELECT statement.
    debug_assert_ne!(statement.num_result_cols().unwrap(), 0);

    // Safe: `statement` is in Cursor state.
    let cursor = unsafe { CursorImpl::new(statement) };

    Ok(cursor)
}

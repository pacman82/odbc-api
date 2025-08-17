use std::sync::{Arc, Mutex};

use crate::{
    Connection, CursorImpl, Error, ParameterCollectionRef,
    handles::{StatementConnection, StatementParent},
};

/// A convinient type alias in case you want to use a connection from multiple threads which share
/// ownership of it.
pub type SharedConnection<'env> = Arc<Mutex<Connection<'env>>>;

/// # Safety:
///
/// Connection is guaranteed to be alive and in connected state for the lifetime of
/// [`SharedConnection`].
unsafe impl StatementParent for SharedConnection<'_> {}

/// Similar to [`crate::Connection::into_cursor`], yet it operates on an `Arc<Mutex<Connection>>`.
/// `Arc<Connection>` can be used if you want shared ownership of connections. However,
/// `Arc<Connection>` is not `Send` due to `Connection` not being `Sync`. So sometimes you may want
/// to wrap your `Connection` into an `Arc<Mutex<Connection>>` to allow shared ownership of the
/// connection across threads. This function allows you to create a cursor from such a shared
/// which also holds a strong reference to it.
///
/// # Parameters
///
/// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;".
/// * `params`: `?` may be used as a placeholder in the statement text. You can use `()` to
///   represent no parameters. See the [`crate::parameter`] module level documentation for more
///   information on how to pass parameters.
/// * `query_timeout_sec`: Use this to limit the time the query is allowed to take, before
///   responding with data to the application. The driver may replace the number of seconds you
///   provide with a minimum or maximum value.
///
///   For the timeout to work the driver must support this feature. E.g. PostgreSQL, and Microsoft
///   SQL Server do, but SQLite or MariaDB do not.
///
///   You can specify ``0``, to deactivate the timeout, this is the default. So if you want no
///   timeout, just leave it at `None`. Only reason to specify ``0`` is if for some reason your
///   datasource does not have ``0`` as default.
///
///   This corresponds to `SQL_ATTR_QUERY_TIMEOUT` in the ODBC C API.
///
///   See: <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetstmtattr-function>
pub fn shared_connection_into_cursor<'env>(
    connection: SharedConnection<'env>,
    query: &str,
    params: impl ParameterCollectionRef,
    query_timeout_sec: Option<usize>,
) -> Result<Option<CursorImpl<StatementConnection<SharedConnection<'env>>>>, Error> {
    let guard = connection
        .lock()
        .expect("Shared connection lock must not be poisned");
    let Some(cursor) = guard.execute(query, params, query_timeout_sec)? else {
        return Ok(None);
    };
    // Deconstrurt the cursor and construct which only borrows the connection and construct a new
    // one which takes ownership of the instead.
    let stmt_ptr = cursor.into_stmt().into_sys();
    drop(guard);
    // Safe: The connection is the parent of the statement referenced by `stmt_ptr`.
    let stmt = unsafe { StatementConnection::new(stmt_ptr, connection) };
    // Safe: `stmt` is valid and in cursor state.
    let cursor = unsafe { CursorImpl::new(stmt) };
    Ok(Some(cursor))
}

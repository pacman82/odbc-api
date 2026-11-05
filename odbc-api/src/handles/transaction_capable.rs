/// Level of transaction support offered by a data source. Mirrors the values reported for the
/// `SQL_TXN_CAPABLE` information type of `SQLGetInfo`.
///
/// See:
/// <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function>
#[derive(Clone, Copy, Hash, Debug, Eq, PartialEq)]
pub enum TransactionCapable {
    /// Transactions are not supported (`SQL_TC_NONE`).
    None,
    /// Transactions can only contain data manipulation statements (DML). Data definition
    /// statements (DDL) encountered in a transaction cause an error (`SQL_TC_DML`).
    Dml,
    /// Transactions can contain DML and DDL statements (`SQL_TC_ALL`).
    All,
    /// Transactions can only contain DML statements. DDL statements encountered in a transaction
    /// cause the transaction to be committed (`SQL_TC_DDL_COMMIT`).
    DdlCommit,
    /// Transactions can only contain DML statements. DDL statements encountered in a transaction
    /// are ignored (`SQL_TC_DDL_IGNORE`).
    DdlIgnore,
}

impl TransactionCapable {
    /// Construct a new instance from the value reported by the driver for `SQL_TXN_CAPABLE`.
    /// Returns `None` for values not defined by the ODBC standard.
    ///
    /// ```
    /// use odbc_api::TransactionCapable;
    ///
    /// assert_eq!(Some(TransactionCapable::None), TransactionCapable::new(0));
    /// assert_eq!(Some(TransactionCapable::Dml), TransactionCapable::new(1));
    /// assert_eq!(Some(TransactionCapable::All), TransactionCapable::new(2));
    /// assert_eq!(Some(TransactionCapable::DdlCommit), TransactionCapable::new(3));
    /// assert_eq!(Some(TransactionCapable::DdlIgnore), TransactionCapable::new(4));
    /// assert_eq!(None, TransactionCapable::new(42));
    /// ```
    pub fn new(value: u16) -> Option<Self> {
        // Values of `SQL_TC_*` as defined in `sql.h`.
        match value {
            0 => Some(TransactionCapable::None),
            1 => Some(TransactionCapable::Dml),
            2 => Some(TransactionCapable::All),
            3 => Some(TransactionCapable::DdlCommit),
            4 => Some(TransactionCapable::DdlIgnore),
            _ => None,
        }
    }
}

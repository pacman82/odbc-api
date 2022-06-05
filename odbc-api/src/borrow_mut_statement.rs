use crate::{
    handles::{Statement, StatementImpl, StatementRef},
    statement_connection::StatementConnection,
};

/// Allows us to be generic over the ownership type (mutably borrowed or owned) of a statement
pub trait AsStatementRef {
    /// An instantiation of [`StatementImpl`]. Allows us to be generic of lifetime of borrowed
    /// connection or ownership thereof.
    type Statement: Statement;

    fn as_stmt_ref(&mut self) -> StatementRef<'_>;
}

impl<'o> AsStatementRef for StatementImpl<'o> {
    type Statement = StatementImpl<'o>;

    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        self.as_stmt_ref()
    }
}

impl<'o> AsStatementRef for &mut StatementImpl<'o> {
    type Statement = StatementImpl<'o>;

    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        (*self).as_stmt_ref()
    }
}

impl<'o> AsStatementRef for StatementConnection<'o> {
    type Statement = StatementConnection<'o>;

    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        self.as_stmt_ref()
    }
}

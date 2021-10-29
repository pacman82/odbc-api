use crate::{
    handles::{Statement, StatementImpl},
    statement_connection::StatementConnection,
};

/// Allows us to be generic over the ownership type (mutably borrowed or owned) of a statement
pub trait BorrowMutStatement {
    /// An instantiation of [`StatementImpl`]. Allows us to be generic of lifetime of borrowed
    /// connection or ownership thereof.
    type Statement: Statement;

    fn borrow(&self) -> &Self::Statement;

    fn borrow_mut(&mut self) -> &mut Self::Statement;
}

impl<'o> BorrowMutStatement for StatementImpl<'o> {
    type Statement = StatementImpl<'o>;

    fn borrow(&self) -> &Self::Statement {
        self
    }

    fn borrow_mut(&mut self) -> &mut Self::Statement {
        self
    }
}

impl<'o> BorrowMutStatement for &mut StatementImpl<'o> {
    type Statement = StatementImpl<'o>;

    fn borrow(&self) -> &Self::Statement {
        self
    }

    fn borrow_mut(&mut self) -> &mut Self::Statement {
        self
    }
}

impl<'o> BorrowMutStatement for StatementConnection<'o> {
    type Statement = StatementConnection<'o>;

    fn borrow(&self) -> &Self::Statement {
        self
    }

    fn borrow_mut(&mut self) -> &mut Self::Statement {
        self
    }
}

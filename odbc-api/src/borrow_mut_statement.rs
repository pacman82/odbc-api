use crate::{
    handles::{Statement, StatementImpl},
    statement_with_connection::StatementWithConnection,
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

impl<'o> BorrowMutStatement for StatementWithConnection<'o> {
    type Statement = StatementWithConnection<'o>;

    fn borrow(&self) -> &Self::Statement {
        self
    }

    fn borrow_mut(&mut self) -> &mut Self::Statement {
        self
    }
}

use widestring::{U16Str, U16String};

use crate::{
    execute::execute_with_parameters, handles::Statement, CursorImpl, Error, ParameterCollection,
};

pub struct Preallocated<'open_connection> {
    statement: Statement<'open_connection>,
}

impl<'o> Preallocated<'o> {
    pub(crate) fn new(statement: Statement<'o>) -> Self {
        Self { statement }
    }

    pub fn execute_utf16(
        &mut self,
        query: &U16Str,
        params: impl ParameterCollection,
    ) -> Result<Option<CursorImpl<'o, &mut Statement<'o>>>, Error> {
        execute_with_parameters(move || Ok(&mut self.statement), Some(query), params)
    }

    pub fn execute(
        &mut self,
        query: &str,
        params: impl ParameterCollection,
    ) -> Result<Option<CursorImpl<'o, &mut Statement<'o>>>, Error> {
        let query = U16String::from_str(query);
        self.execute_utf16(&query, params)
    }
}

use crate::{
    execute::execute,
    handles::{Statement, StatementImpl},
    parameter::StableCData,
    CursorImpl, Error, ParameterRefCollection,
};

/// A prepared statement with prebound parameters.
///
/// Helps you avoid (potentially costly) ODBC function calls than repeatedly executing the same
/// prepared query with different parameters. Using this instead of [`crate::Prepared::execute`]
/// directly results in the parameter buffers only to be bound once and modified between calls,
/// instead of new buffers bound.
pub struct Prebound<'open_connection, Parameters> {
    statement: StatementImpl<'open_connection>,
    parameters: Parameters,
}

impl<'o, P> Prebound<'o, P>
where
    P: ParameterRefCollection + StableMutRef,
{
    /// # Safety
    ///
    /// * `statement` must be a prepared statement without any parameters bound.
    /// * `parameters` bound to statement. Must not be invalidated by moving. Nor should any
    ///   operations which can be performed through a mutable reference be able to invalide the
    ///   binding.
    pub unsafe fn new(mut statement: StatementImpl<'o>, mut parameters: P) -> Result<Self, Error> {
        statement.reset_parameters().into_result(&statement)?;
        let paramset_size = parameters.parameter_set_size();
        statement.set_paramset_size(paramset_size);
        parameters.bind_parameters_to(&mut statement)?;
        Ok(Self {
            statement,
            parameters,
        })
    }

    /// Execute the prepared statement
    pub fn execute(&mut self) -> Result<Option<CursorImpl<&mut StatementImpl<'o>>>, Error> {
        unsafe { execute(&mut self.statement, None) }
    }

    /// Provides write access to the bound parameters. Used to change arguments betwenn statement
    /// executions.
    pub fn params_mut(&mut self) -> &mut P::Mut {
        self.parameters.as_mut()
    }
}

/// # Safety
///
/// The changes made through the reference returned by `as_mut` may not invalidate the parameter
/// pointers bound to a statement.
pub unsafe trait StableMutRef {
    /// Mutable projection used to change parameter values in between statement executions.
    type Mut;

    /// Acquire a mutable projection of the parameters to change values between executions of the
    /// statement.
    fn as_mut(&mut self) -> &mut Self::Mut;
}

unsafe impl<T> StableMutRef for &mut T where T: StableCData,
{
    type Mut = T;

    fn as_mut(&mut self) -> &mut T {
        self
    }
}

unsafe impl<T> StableMutRef for Box<T> where T: StableCData,
{
    type Mut = T;

    fn as_mut(&mut self) -> &mut T {
        self
    }
}
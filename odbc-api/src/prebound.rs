use crate::{
    execute::execute,
    handles::{Statement, StatementImpl},
    parameter::StableCData,
    CursorImpl, Error, ParameterCollection,
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
    P: PinnedParameterCollection,
{
    /// # Safety
    ///
    /// * `statement` must be a prepared statement.
    /// * `parameters` will be bound to statement. Must not be invalidated by moving. Nor should any
    ///   operations which can be performed through a mutable reference be able to invalidate the
    ///   bindings.
    pub unsafe fn new(mut statement: StatementImpl<'o>, mut parameters: P) -> Result<Self, Error> {
        statement.reset_parameters().into_result(&statement)?;
        let ref_parameters = parameters.deref();
        let paramset_size = ref_parameters.parameter_set_size();
        statement.set_paramset_size(paramset_size);
        ref_parameters.bind_parameters_to(&mut statement)?;
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
    pub fn params_mut(&mut self) -> &mut P::ViewMut {
        self.parameters.as_view_mut()
    }
}

/// Smart Pointer to a parameter collection, which allows mutating buffer content and can be moved
/// without invalidating bound pointers. The methods in this trait are intended [`self::Prebound`]
/// and not intended for end users to be called directly.
///
/// # Safety
///
/// * Changes made through [`Self::as_mut`] must not invalidate or change the pointers, of
///   [`Self::Target`] E.g. reallocate the referenced buffers.
/// * Moving this smart pointer must not move any bound buffers in memory.
pub unsafe trait PinnedParameterCollection {
    /// The target parameters are intended to be bound to a statement and mutated / read between
    /// execution.
    type Target: ParameterCollection;

    /// Mutable projection used to change parameter values in between statement executions. It is
    /// intended to allow changing the parameters in between statement execution. It must not be
    /// possible to perfom any operations on the [`Self::Target`] using this view, which would
    /// invalidate any of the pointers already bound to a [`self::Prebound.`]
    type ViewMut;

    /// Dereference parameters for binding.
    unsafe fn deref(&mut self) -> &mut Self::Target;

    /// Acquire a mutable projection of the parameters to change values between executions of the
    /// statement.
    fn as_view_mut(&mut self) -> &mut Self::ViewMut;
}

unsafe impl<T> PinnedParameterCollection for &mut T
where
    T: StableCData + ParameterCollection,
{
    type ViewMut = T;

    type Target = T;

    fn as_view_mut(&mut self) -> &mut T {
        self
    }

    unsafe fn deref(&mut self) -> &mut Self::Target {
        self
    }
}

unsafe impl<T> PinnedParameterCollection for Box<T>
where
    T: StableCData + ParameterCollection,
{
    type ViewMut = T;

    type Target = T;

    fn as_view_mut(&mut self) -> &mut T {
        self
    }

    unsafe fn deref(&mut self) -> &mut T {
        self
    }
}

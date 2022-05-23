use crate::{
    execute::execute,
    fixed_sized::Pod,
    handles::{Statement, StatementImpl},
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
    /// Avoid using this unsafe constructor. New instances of this are best constructed using
    /// [`crate::Prepared::bind_parameters`].
    ///
    /// # Safety
    ///
    /// * `statement` must be a prepared statement.
    /// * `parameters` will be bound to statement. Must not be invalidated by moving. Nor should any
    ///   operations which can be performed through a mutable reference be able to invalidate the
    ///   bindings.
    pub unsafe fn new(mut statement: StatementImpl<'o>, mut parameters: P) -> Result<Self, Error> {
        statement.reset_parameters().into_result(&statement)?;
        let ref_parameters = parameters.deref();
        ref_parameters.bind_parameters_to(&mut statement)?;
        Ok(Self {
            statement,
            parameters,
        })
    }

    /// Execute the prepared statement
    pub fn execute(&mut self) -> Result<Option<CursorImpl<&mut StatementImpl<'o>>>, Error> {
        unsafe {
            let ref_parameters = self.parameters.deref();
            // We reset the parameter set size, in order to adequatly handle batches of different
            // size then inserting into the database.
            let paramset_size = ref_parameters.parameter_set_size();
            if paramset_size == 0 {
                // A batch size of 0 will not execute anything, same as for execute on connection or
                // prepared.
                Ok(None)
            } else {
                self.statement.set_paramset_size(paramset_size);
                execute(&mut self.statement, None)
            }
        }
    }
}

impl<'o, P> Prebound<'o, P> {
    /// Provides write access to the bound parameters. Used to change arguments between statement
    /// executions.
    pub fn params_mut<'a>(&'a mut self) -> P::ViewMut
    where
        P: ViewPinnedMut<'a, 'o>,
    {
        self.parameters.as_view_mut(&mut self.statement)
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

    /// Dereference parameters for binding.
    ///
    /// # Safety
    ///
    /// This reference could be used to invalidate pointers bound to the statement. There is nothing
    /// protecing users from doing so. It is the callers responsibility to not do any such thing
    /// with the returned reference. Best only to use this reference to bind [`Self::Target`] to a
    /// statement.
    unsafe fn deref(&mut self) -> &mut Self::Target;
}

/// Ability to change the contents of a bound buffer through a mutable view, without invalidating
/// its pointers.
///
/// # Safety
///
/// * Changes made through [`Self::as_view_mut`] must either not invalidate or change the pointers
///   bound to a statement previously, or they must use the statement handle to rebind valid
///   pointers. At the latest once the view is dropped.
pub unsafe trait ViewPinnedMut<'a, 'o> {
    /// Mutable projection used to change parameter values in between statement executions. It is
    /// intended to allow changing the parameters in between statement execution. It must not be
    /// possible to perfom any operations on the [`Self::Target`] using this view, which would
    /// invalidate any of the pointers already bound to a [`self::Prebound.`]
    type ViewMut;

    /// Acquire a mutable projection of the parameters to change values between executions of the
    /// statement.
    fn as_view_mut(&'a mut self, stmt: &'a mut StatementImpl<'o>) -> Self::ViewMut;
}

// &mut T

unsafe impl<T> PinnedParameterCollection for &mut T
where
    T: ParameterCollection,
{
    type Target = T;

    unsafe fn deref(&mut self) -> &mut Self::Target {
        self
    }
}

unsafe impl<'a, 'b: 'a, 'o, T> ViewPinnedMut<'a, 'o> for &'b mut T
where
    T: Pod,
{
    type ViewMut = &'a mut T;

    fn as_view_mut(&'a mut self, _stmt: &'a mut StatementImpl<'o>) -> &'a mut T {
        self
    }
}

// Box<T>

unsafe impl<T> PinnedParameterCollection for Box<T>
where
    T: ParameterCollection,
{
    type Target = T;

    unsafe fn deref(&mut self) -> &mut T {
        self
    }
}

unsafe impl<'a, 'o, T> ViewPinnedMut<'a, 'o> for Box<T>
where
    T: Pod,
{
    type ViewMut = &'a mut T;

    fn as_view_mut(&'a mut self, _stmt: &'a mut StatementImpl<'o>) -> &'a mut T {
        self
    }
}

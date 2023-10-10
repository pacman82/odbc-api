use crate::{handles::Statement, parameter::InputParameter, Error};

mod tuple;

pub use tuple::ParameterTupleElement;

/// A collection of input parameters. They can be bound to a statement using a shared reference.
///
/// # Safety
///
/// Must only bind pointers to statement which are valid for the lifetime of the collection. The
/// parameter set size returned must not be larger than the range of valid values behind these
/// pointers.
pub unsafe trait InputParameterCollection {
    /// Number of values per parameter in the collection. This can be different from the maximum
    /// batch size a buffer may be able to hold. Returning `0` will cause the the query not to be
    /// executed.
    fn parameter_set_size(&self) -> usize;

    /// # Safety
    ///
    /// On execution a statement may want to read/write to the bound paramaters. It is the callers
    /// responsibility that by then the buffers are either unbound from the statement or still
    /// valid.
    unsafe fn bind_input_parameters_to(&self, stmt: &mut impl Statement) -> Result<(), Error>;
}

unsafe impl<T> ParameterCollectionRef for &T
where
    T: InputParameterCollection + ?Sized,
{
    fn parameter_set_size(&self) -> usize {
        (**self).parameter_set_size()
    }

    unsafe fn bind_parameters_to(&mut self, stmt: &mut impl Statement) -> Result<(), Error> {
        self.bind_input_parameters_to(stmt)
    }
}

unsafe impl<T> InputParameterCollection for T
where
    T: InputParameter + ?Sized,
{
    fn parameter_set_size(&self) -> usize {
        1
    }

    unsafe fn bind_input_parameters_to(&self, stmt: &mut impl Statement) -> Result<(), Error> {
        self.assert_completness();
        stmt.bind_input_parameter(1, self).into_result(stmt)
    }
}

unsafe impl<T> InputParameterCollection for [T]
where
    T: InputParameter,
{
    fn parameter_set_size(&self) -> usize {
        1
    }

    unsafe fn bind_input_parameters_to(&self, stmt: &mut impl Statement) -> Result<(), Error> {
        for (index, parameter) in self.iter().enumerate() {
            parameter.assert_completness();
            stmt.bind_input_parameter(index as u16 + 1, parameter)
                .into_result(stmt)?;
        }
        Ok(())
    }
}

/// SQL Parameters used to execute a query.
///
/// ODBC allows to place question marks (`?`) in the statement text as placeholders. For each such
/// placeholder a parameter needs to be bound to the statement before executing it.
///
/// # Examples
///
/// This trait is implemented by single parameters.
///
/// ```no_run
/// use odbc_api::{Environment, ConnectionOptions};
///
/// let env = Environment::new()?;
///
/// let mut conn = env.connect(
///     "YourDatabase", "SA", "My@Test@Password1",
///     ConnectionOptions::default()
/// )?;
/// let year = 1980;
/// if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays WHERE year > ?;", &year)? {
///     // Use cursor to process query results.
/// }
/// # Ok::<(), odbc_api::Error>(())
/// ```
///
/// Tuples of `Parameter`s implement this trait, too.
///
/// ```no_run
/// use odbc_api::{Environment, ConnectionOptions};
///
/// let env = Environment::new()?;
///
/// let mut conn = env.connect(
///     "YourDatabase", "SA", "My@Test@Password1",
///     ConnectionOptions::default()
/// )?;
/// let too_old = 1980;
/// let too_young = 2000;
/// if let Some(cursor) = conn.execute(
///     "SELECT year, name FROM Birthdays WHERE ? < year < ?;",
///     (&too_old, &too_young),
/// )? {
///     // Use cursor to congratulate only persons in the right age group...
/// }
/// # Ok::<(), odbc_api::Error>(())
/// ```
///
/// And so do array slices of `Parameter`s.
///
/// ```no_run
/// use odbc_api::{Environment, ConnectionOptions};
///
/// let env = Environment::new()?;
///
/// let mut conn = env.connect(
///     "YourDatabase",
///     "SA",
///     "My@Test@Password1",
///     ConnectionOptions::default()
/// )?;
/// let params = [1980, 2000];
/// if let Some(cursor) = conn.execute(
///     "SELECT year, name FROM Birthdays WHERE ? < year < ?;",
///     &params[..])?
/// {
///     // Use cursor to process query results.
/// }
/// # Ok::<(), odbc_api::Error>(())
/// ```
///
/// # Safety
///
/// Instances of this type are passed by value, so this type can be implemented by both constant and
/// mutable references. Implementers should take care that the values bound by `bind_parameters_to`
/// to the statement live at least for the Duration of `self`. The most straight forward way of
/// achieving this is of course, to bind members.
pub unsafe trait ParameterCollectionRef {
    /// Number of values per parameter in the collection. This can be different from the maximum
    /// batch size a buffer may be able to hold. Returning `0` will cause the the query not to be
    /// executed.
    fn parameter_set_size(&self) -> usize;

    /// # Safety
    ///
    /// On execution a statement may want to read/write to the bound paramaters. It is the callers
    /// responsibility that by then the buffers are either unbound from the statement or still
    /// valild.
    unsafe fn bind_parameters_to(&mut self, stmt: &mut impl Statement) -> Result<(), Error>;
}

unsafe impl<T> ParameterCollectionRef for &mut T
where
    T: ParameterCollection,
{
    fn parameter_set_size(&self) -> usize {
        (**self).parameter_set_size()
    }

    unsafe fn bind_parameters_to(&mut self, stmt: &mut impl Statement) -> Result<(), Error> {
        (**self).bind_parameters_to(stmt)
    }
}

/// Implementers of this trait can be bound to a statement through a
/// [`self::ParameterCollectionRef`].
///
/// # Safety
///
/// Parameters bound to the statement must remain valid for the lifetime of the instance.
pub unsafe trait ParameterCollection {
    /// Number of values per parameter in the collection. This can be different from the maximum
    /// batch size a buffer may be able to hold. Returning `0` will cause the the query not to be
    /// executed.
    fn parameter_set_size(&self) -> usize;

    /// Bind the parameters to a statement
    ///
    /// # Safety
    ///
    /// Since the parameter is now bound to `stmt` callers must take care that it is ensured that
    /// the parameter remains valid while it is used. If the parameter is bound as an output
    /// parameter it must also be ensured that it is exclusively referenced by statement.
    unsafe fn bind_parameters_to(&mut self, stmt: &mut impl Statement) -> Result<(), Error>;
}

unsafe impl<T> ParameterCollection for T
where
    T: InputParameterCollection + ?Sized,
{
    fn parameter_set_size(&self) -> usize {
        (*self).parameter_set_size()
    }

    unsafe fn bind_parameters_to(&mut self, stmt: &mut impl Statement) -> Result<(), Error> {
        self.bind_input_parameters_to(stmt)
    }
}

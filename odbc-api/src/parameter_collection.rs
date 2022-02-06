use crate::{handles::Statement, parameter::InputParameter, Error, ParameterRef};

mod tuple;

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
/// use odbc_api::Environment;
///
/// let env = Environment::new()?;
///
/// let mut conn = env.connect("YourDatabase", "SA", "My@Test@Password1")?;
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
/// use odbc_api::Environment;
///
/// let env = Environment::new()?;
///
/// let mut conn = env.connect("YourDatabase", "SA", "My@Test@Password1")?;
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
/// use odbc_api::Environment;
///
/// let env = Environment::new()?;
///
/// let mut conn = env.connect("YourDatabase", "SA", "My@Test@Password1")?;
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
/// mutabale references. Implementers should take care that the values bound by `bind_parameters_to`
/// to the statement live at least for the Duration of `self`. The most straight forward way of
/// achieving this is of course, to bind members.
pub unsafe trait ParameterRefCollection {
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

unsafe impl<T> ParameterRefCollection for T
where
    T: ParameterRef,
{
    fn parameter_set_size(&self) -> usize {
        1
    }

    unsafe fn bind_parameters_to(&mut self, stmt: &mut impl Statement) -> Result<(), Error> {
        self.bind_to(1, stmt)
    }
}

unsafe impl<T> ParameterRefCollection for &[T]
where
    T: InputParameter,
{
    fn parameter_set_size(&self) -> usize {
        1
    }

    unsafe fn bind_parameters_to(&mut self, stmt: &mut impl Statement) -> Result<(), Error> {
        for (index, parameter) in self.iter().enumerate() {
            stmt.bind_input_parameter(index as u16 + 1, parameter)
                .into_result(stmt)?;
        }
        Ok(())
    }
}

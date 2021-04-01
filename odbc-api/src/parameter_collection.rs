use crate::{
    handles::{Statement, StatementImpl},
    Error, InputParameter, Parameter,
};

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
/// let env = unsafe {
///     Environment::new()?
/// };
///
/// let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
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
/// let env = unsafe {
///     Environment::new()?
/// };
///
/// let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
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
/// let env = unsafe {
///     Environment::new()?
/// };
///
/// let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
/// let params = [1980, 2000];
/// if let Some(cursor) = conn.execute(
///     "SELECT year, name FROM Birthdays WHERE ? < year < ?;",
///     &params[..])?
/// {
///     // Use cursor to process query results.
/// }
/// # Ok::<(), odbc_api::Error>(())
/// ```
pub unsafe trait ParameterCollection {
    /// Number of values per parameter in the collection. This can be different from the maximum
    /// batch size a buffer may be able to hold. Returning `0` will cause the the query not to be
    /// executed.
    fn parameter_set_size(&self) -> u32;

    /// # Safety
    ///
    /// Implementers should take care that the values bound by this method to the statement live at
    /// least for the Duration of `self`. The most straight forward way of achieving this is of
    /// course, to bind members.
    unsafe fn bind_parameters_to(self, stmt: &mut StatementImpl<'_>) -> Result<(), Error>;
}

unsafe impl<T> ParameterCollection for T
where
    T: Parameter,
{
    fn parameter_set_size(&self) -> u32 {
        1
    }

    unsafe fn bind_parameters_to(self, stmt: &mut StatementImpl<'_>) -> Result<(), Error> {
        self.bind_parameter(1, stmt)
    }
}

unsafe impl<T> ParameterCollection for &[T]
where
    T: InputParameter,
{
    fn parameter_set_size(&self) -> u32 {
        1
    }

    unsafe fn bind_parameters_to(self, stmt: &mut StatementImpl<'_>) -> Result<(), Error> {
        for (index, parameter) in self.iter().enumerate() {
            stmt.bind_input_parameter(index as u16 + 1, parameter)?;
        }
        Ok(())
    }
}

//! Implement `Parameters` trait for tuples consisting of elements implementing `SingleParameter`
//! trait.

use super::ParameterCollectionRef;
use crate::{handles::Statement, parameter::InputParameter, Error, InOut, Out, OutputParameter};

macro_rules! impl_bind_parameters {
    ($offset:expr, $stmt:ident) => (
        Ok(())
    );
    ($offset:expr, $stmt:ident $head:ident $($tail:ident)*) => (
        {
            $head.bind_to($offset+1, $stmt)?;
            impl_bind_parameters!($offset+1, $stmt $($tail)*)
        }
    );
}

macro_rules! impl_parameters_for_tuple{
    ($($t:ident)*) => (
        #[allow(unused_parens)]
        #[allow(unused_variables)]
        #[allow(non_snake_case)]
        unsafe impl<$($t:ParameterTupleElement,)*> ParameterCollectionRef for ($($t,)*)
        {
            fn parameter_set_size(&self) -> usize {
                1
            }

            unsafe fn bind_parameters_to(&mut self, stmt: &mut impl Statement) -> Result<(), Error> {
                let ($($t,)*) = self;
                impl_bind_parameters!(0, stmt $($t)*)
            }
        }
    );
}

// The unit type is used to signal no parameters.
impl_parameters_for_tuple! {}
impl_parameters_for_tuple! { A }
impl_parameters_for_tuple! { A B }
impl_parameters_for_tuple! { A B C }
impl_parameters_for_tuple! { A B C D }
impl_parameters_for_tuple! { A B C D E }
impl_parameters_for_tuple! { A B C D E F }
impl_parameters_for_tuple! { A B C D E F G }
impl_parameters_for_tuple! { A B C D E F G H }
impl_parameters_for_tuple! { A B C D E F G H I }
impl_parameters_for_tuple! { A B C D E F G H I J }
impl_parameters_for_tuple! { A B C D E F G H I J K }
impl_parameters_for_tuple! { A B C D E F G H I J K L }
impl_parameters_for_tuple! { A B C D E F G H I J K L M }
impl_parameters_for_tuple! { A B C D E F G H I J K L M N }
impl_parameters_for_tuple! { A B C D E F G H I J K L M N O }
impl_parameters_for_tuple! { A B C D E F G H I J K L M N O P }
impl_parameters_for_tuple! { A B C D E F G H I J K L M N O P Q }
impl_parameters_for_tuple! { A B C D E F G H I J K L M N O P Q R }
impl_parameters_for_tuple! { A B C D E F G H I J K L M N O P Q R S }
impl_parameters_for_tuple! { A B C D E F G H I J K L M N O P Q R S T }
impl_parameters_for_tuple! { A B C D E F G H I J K L M N O P Q R S T U }
impl_parameters_for_tuple! { A B C D E F G H I J K L M N O P Q R S T U V }
impl_parameters_for_tuple! { A B C D E F G H I J K L M N O P Q R S T U V W }
impl_parameters_for_tuple! { A B C D E F G H I J K L M N O P Q R S T U V W X }
impl_parameters_for_tuple! { A B C D E F G H I J K L M N O P Q R S T U V W X Y }
impl_parameters_for_tuple! { A B C D E F G H I J K L M N O P Q R S T U V W X Y Z}

/// Implementers of this trait can be used as individual parameters of in a tuple of parameter
/// references. This includes input parameters, output or in/out parameters.
///
/// # Safety
///
/// Parameters bound to the statement must remain valid for the lifetime of the instance.
pub unsafe trait ParameterTupleElement {
    /// Bind the parameter in question to a specific `parameter_number`.
    ///
    /// # Safety
    ///
    /// Since the parameter is now bound to `stmt` callers must take care that it is ensured that
    /// the parameter remains valid while it is used. If the parameter is bound as an output
    /// parameter it must also be ensured that it is exclusively referenced by statement.
    unsafe fn bind_to(
        &mut self,
        parameter_number: u16,
        stmt: &mut impl Statement,
    ) -> Result<(), Error>;
}

/// Bind immutable references as input parameters.
unsafe impl<T: ?Sized> ParameterTupleElement for &T
where
    T: InputParameter,
{
    unsafe fn bind_to(
        &mut self,
        parameter_number: u16,
        stmt: &mut impl Statement,
    ) -> Result<(), Error> {
        self.assert_completness();
        stmt.bind_input_parameter(parameter_number, *self)
            .into_result(stmt)
    }
}

/// Bind mutable references as input/output parameter.
unsafe impl<T> ParameterTupleElement for InOut<'_, T>
where
    T: OutputParameter + InputParameter,
{
    unsafe fn bind_to(
        &mut self,
        parameter_number: u16,
        stmt: &mut impl Statement,
    ) -> Result<(), Error> {
        self.0.assert_completness();
        stmt.bind_parameter(parameter_number, odbc_sys::ParamType::InputOutput, self.0)
            .into_result(stmt)
    }
}

/// Mutable references wrapped in `Out` are bound as output parameters.
unsafe impl<T> ParameterTupleElement for Out<'_, T>
where
    T: OutputParameter,
{
    unsafe fn bind_to(
        &mut self,
        parameter_number: u16,
        stmt: &mut impl Statement,
    ) -> Result<(), Error> {
        stmt.bind_parameter(parameter_number, odbc_sys::ParamType::Output, self.0)
            .into_result(stmt)
    }
}

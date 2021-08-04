//! Implement `Parameters` trait for tuples consisting of elements implementing `SingleParameter`
//! trait.

use super::ParameterCollection;
use crate::{handles::StatementImpl, Error, Parameter};

macro_rules! impl_bind_parameters {
    ($offset:expr, $stmt:ident) => (
        Ok(())
    );
    ($offset:expr, $stmt:ident $head:ident $($tail:ident)*) => (
        {
            $head.bind_parameter($offset+1, $stmt)?;
            impl_bind_parameters!($offset+1, $stmt $($tail)*)
        }
    );
}

macro_rules! impl_parameters_for_tuple{
    ($($t:ident)*) => (
        #[allow(unused_parens)]
        #[allow(unused_variables)]
        #[allow(non_snake_case)]
        unsafe impl<$($t:Parameter,)*> ParameterCollection for ($($t,)*)
        {
            fn parameter_set_size(&self) -> usize {
                1
            }

            unsafe fn bind_parameters_to(self, stmt: &mut StatementImpl<'_>) -> Result<(), Error> {
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

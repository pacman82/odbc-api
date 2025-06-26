mod expand;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

use crate::expand::expand;

/// Use this to derive the trait `FetchRow` for structs defined in the application logic.
///
/// # Example
///
/// ```
/// use odbc_api_derive::Fetch;
/// use odbc_api::{Connection, Error, Cursor, parameter::VarCharArray, buffers::RowVec};
///
/// #[derive(Default, Clone, Copy, Fetch)]
/// struct Person {
///     first_name: VarCharArray<255>,
///     last_name: VarCharArray<255>,
/// }
///
/// fn send_greetings(conn: &mut Connection) -> Result<(), Error> {
///     let max_rows_in_batch = 250;
///     let buffer = RowVec::<Person>::new(max_rows_in_batch);
///     let mut cursor = conn.execute("SELECT first_name, last_name FROM Persons", (), None)?
///         .expect("SELECT must yield a result set");
///     let mut block_cursor = cursor.bind_buffer(buffer)?;
///
///     while let Some(batch) = block_cursor.fetch()? {
///         for person in batch.iter() {
///             let first = person.first_name.as_str()
///                 .expect("First name must be UTF-8")
///                 .expect("First Name must not be NULL");
///             let last = person.last_name.as_str()
///                 .expect("Last name must be UTF-8")
///                 .expect("Last Name must not be NULL");
///             println!("Hello {first} {last}!")
///         }
///     }
///     Ok(())
/// }
/// ```
#[proc_macro_derive(Fetch)]
pub fn derive_fetch_row(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let output = expand(input);
    proc_macro::TokenStream::from(output)
}

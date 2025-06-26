use proc_macro2::TokenStream;
use quote::quote;
use syn::DeriveInput;

pub fn expand(input: DeriveInput) -> TokenStream {
    let struct_name = input.ident;

    let struct_data = match input.data {
        syn::Data::Struct(struct_data) => struct_data,
        _ => panic!("Fetch can only be derived for structs"),
    };

    let fields = struct_data.fields;

    let bindings = fields.iter().enumerate().map(|(index, field)| {
        let field_name = field
            .ident
            .as_ref()
            .expect("All struct members must be named");
        let col_index = (index + 1) as u16;
        quote! {
            odbc_api::buffers::FetchRowMember::bind_to_col(
                &mut self.#field_name,
                #col_index,
                &mut cursor
            )?;
        }
    });

    let find_truncation = fields.iter().enumerate().map(|(index, field)| {
        let field_name = field
            .ident
            .as_ref()
            .expect("All struct members must be named");
        quote! {
            let maybe_truncation = odbc_api::buffers::FetchRowMember::find_truncation(
                &self.#field_name,
                #index,
            );
            if let Some(truncation_info) = maybe_truncation {
                return Some(truncation_info);
            }
        }
    });

    let expanded = quote! {
        unsafe impl odbc_api::buffers::FetchRow for #struct_name {

            unsafe fn bind_columns_to_cursor(
                &mut self,
                mut cursor: odbc_api::handles::StatementRef<'_>
            ) -> std::result::Result<(), odbc_api::Error> {
                #(#bindings)*
                Ok(())
            }

            fn find_truncation(&self) -> std::option::Option<odbc_api::TruncationInfo> {
                #(#find_truncation)*
                None
            }
        }
    };

    expanded.into()
}

#[cfg(test)]
mod tests {
    use proc_macro2::TokenStream;
    use quote::quote;
    use syn::{DeriveInput, parse2};

    use super::expand;

    #[test]
    fn derive_fetch_for_row() {
        let input = given(quote! {
            struct MyRow {
                a: i64,
                b: VarCharArray<50>,
            }
        });

        let output = expand(input);

        let expected = quote! {
            unsafe impl odbc_api::buffers::FetchRow for MyRow {

                unsafe fn bind_columns_to_cursor(
                    &mut self,
                    mut cursor: odbc_api::handles::StatementRef<'_>
                ) -> std::result::Result<(), odbc_api::Error> {
                    odbc_api::buffers::FetchRowMember::bind_to_col(
                        &mut self.a,
                        1u16,
                        &mut cursor
                    )?;
                    odbc_api::buffers::FetchRowMember::bind_to_col(
                        &mut self.b,
                        2u16,
                        &mut cursor
                    )?;
                    Ok(())
                }

                fn find_truncation(&self) -> std::option::Option<odbc_api::TruncationInfo> {
                    let maybe_truncation = odbc_api::buffers::FetchRowMember::find_truncation(
                        &self.a,
                        0usize,
                    );
                    if let Some(truncation_info) = maybe_truncation {
                        return Some(truncation_info);
                    }
                    let maybe_truncation = odbc_api::buffers::FetchRowMember::find_truncation(
                        &self.b,
                        1usize,
                    );
                    if let Some(truncation_info) = maybe_truncation {
                        return Some(truncation_info);
                    }
                    None
                }
            }
        };
        assert_eq!(expected.to_string(), output.to_string());
    }

    fn given(input: TokenStream) -> DeriveInput {
        parse2(input).unwrap()
    }
}

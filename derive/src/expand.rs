use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields};

pub fn expand(input: DeriveInput) -> TokenStream {
    let struct_name = input.ident;

    let Data::Struct(struct_data) = input.data else {
        return quote! { compile_error!("Fetch can only be derived for structs"); };
    };

    let Fields::Named(named_fields) = struct_data.fields else {
        return quote! { compile_error!("Fetch can only be derived for structs with named fields"); };
    };
    let fields = named_fields.named;

    let field_names = || {
        fields.iter().map(|f| {
            f.ident
                .as_ref()
                .expect("All fields in a struct with named fields must be named.")
        })
    };

    let bindings = field_names().enumerate().map(|(index, field_name)| {
        let col_index = (index + 1) as u16;
        quote! {
            odbc_api::buffers::FetchRowMember::bind_to_col(
                &mut self.#field_name,
                #col_index,
                &mut cursor
            )?;
        }
    });

    let find_truncation = field_names().enumerate().map(|(index, field_name)| {
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

    expanded
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

    #[test]
    fn compiler_error_when_deriving_for_enum() {
        let input = given(quote! {
            enum NotAStruct {}
        });

        let output = expand(input);

        let expected = quote! {
            compile_error!("Fetch can only be derived for structs");
        };
        assert_eq!(expected.to_string(), output.to_string());
    }

    #[test]
    fn compiler_error_when_deriving_for_tuple_struct() {
        let input = given(quote! {
            struct TupleStruct(i64, i64);
        });

        let output = expand(input);

        let expected = quote! {
            compile_error!("Fetch can only be derived for structs with named fields");
        };
        assert_eq!(expected.to_string(), output.to_string());
    }

    fn given(input: TokenStream) -> DeriveInput {
        parse2(input).unwrap()
    }
}

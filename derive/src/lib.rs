use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Use this to derive the trait `FetchRow` for structs defined in the application logic.
#[proc_macro_derive(Fetch)]
pub fn derive_fetch_row(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    let struct_name = input.ident;

    let struct_data = match input.data {
        syn::Data::Struct(struct_data) => struct_data,
        _ => panic!("Fetch can only be derived for structs"),
    };

    let fields = struct_data.fields;

    let bindings = fields.iter().enumerate().map(|(index, field)| {
        let field_name = field.ident.as_ref().expect("All struct members must be named");
        let col_index = (index + 1) as u16;
        quote!{
            odbc_api::buffers::FetchRowMember::bind_to_col(
                &mut self.#field_name,
                #col_index,
                &mut cursor
            )?;
        }
    });

    let find_truncation = fields.iter().enumerate().map(|(index, field)| {
        let field_name = field.ident.as_ref().expect("All struct members must be named");
        quote!{
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

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

/// Derives `katha::traits::event_name::EventName`.
///
/// Optional container attribute:
/// - `#[event_name = "ExplicitName"]`
#[proc_macro_derive(EventName, attributes(event_name))]
pub fn derive_event_name(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = &input.ident;

    let explicit_name = input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("event_name"))
        .map(extract_event_name)
        .transpose();

    let event_name = match explicit_name {
        Ok(Some(name)) => name.value(),
        Ok(None) => ident.to_string(),
        Err(error) => return error.to_compile_error().into(),
    };

    quote! {
        impl ::katha::traits::event_name::EventName for #ident {
            const NAME: &'static str = #event_name;
        }
    }
    .into()
}

fn extract_event_name(attr: &syn::Attribute) -> syn::Result<syn::LitStr> {
    match &attr.meta {
        syn::Meta::List(_) => attr.parse_args::<syn::LitStr>(),
        syn::Meta::NameValue(name_value) => {
            if let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(value),
                ..
            }) = &name_value.value
            {
                Ok(value.clone())
            } else {
                Err(syn::Error::new_spanned(
                    &name_value.value,
                    "event_name must be a string literal",
                ))
            }
        }
        syn::Meta::Path(path) => Err(syn::Error::new_spanned(
            path,
            "event_name requires a string value",
        )),
    }
}

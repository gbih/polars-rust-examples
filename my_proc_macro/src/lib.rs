#![allow(unused)]

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse_quote;
use syn::parse_macro_input;
use syn::spanned::Spanned;
use syn::ItemFn;
use syn::File;

#[proc_macro_attribute]
pub fn print_source(_attr: TokenStream, item: TokenStream) -> TokenStream {

    let input = TokenStream2::from(item.clone());
    let mut input_fn = parse_macro_input!(item as ItemFn);
    
    let fn_name = &input_fn.sig.ident;
    let wrapper_name = syn::Ident::new(&format!("{}_wrapper", fn_name), fn_name.span());
    
    // Parse the input as a complete Rust file
    let file: syn::File = syn::parse2(input.clone()).unwrap();

    // Use prettyplease to format the code while preserving original formatting
    let original_code = prettyplease::unparse(&file);

    let return_type = &input_fn.sig.output;
    
    let original_body = input_fn.block;
    input_fn.block = parse_quote!({
        //println!("Function source code:\n\n{}", #original_code);
        println!("\n{}", #original_code);
        #original_body
    });
    
    // Get the function arguments
    let args = input_fn.sig.inputs.iter().collect::<Vec<_>>();
    let arg_names = args.iter().map(|arg| {
        if let syn::FnArg::Typed(pat_type) = arg {
            if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                &pat_ident.ident
            } else {
                panic!("Unsupported argument pattern")
            }
        } else {
            panic!("Unsupported argument type")
        }
    });
    
    let output = quote! {
        #input_fn

        pub fn #wrapper_name(#(#args),*) #return_type {
            #fn_name(#(#arg_names),*)
        }
    };
    
    output.into()
}



// // 1. https://www.perplexity.ai/search/in-rust-given-this-code-in-mai-gj9jHnJTSTiuKjRdnJC.VQ
// // 2. https://www.perplexity.ai/search/1-in-rust-polars-i-have-this-m-GlM9AqLcSiOjnQa3xdX.cw
// // 3. https://www.perplexity.ai/search/1-in-rust-polars-i-have-this-m-dSslGmFmQhuDqK5gyDAS6A
// // 4. https://www.perplexity.ai/search/1-in-rust-polars-i-have-this-m-cGA.G98iRr2Ro1vkseKMeQ
// // 5. https://www.perplexity.ai/search/1-in-rust-polars-i-have-this-m-F3ZhQwB7TUeLTsDN1Va5xg
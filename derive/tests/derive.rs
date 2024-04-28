use odbc_api::parameter::VarCharArray;
use odbc_api_derive::Fetch;

#[derive(Fetch, Clone, Copy)]
struct MyRow {
    a: i64,
    b: VarCharArray<50>
}

#[test]
fn fetch_rowise_derive_integration() {
    
}
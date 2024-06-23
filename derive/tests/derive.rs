use odbc_api::parameter::VarCharArray;
use odbc_api_derive::Fetch;

// A check, wether the derive syntax produces something that compiles. For a test actually fetching
// date from a database using this generated code, run the integration tests of `odbc-api` with the
// `derive` feature activated. We allow dead code here, because we do not intend to invoke the
// implementation here.
#[allow(dead_code)]
#[derive(Fetch, Clone, Copy)]
struct MyRow {
    a: i64,
    b: VarCharArray<50>,
}

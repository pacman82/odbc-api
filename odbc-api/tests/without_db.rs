use odbc_api::buffers::{AnyColumnViewMut, BufferDescription, BufferKind, ColumnarRowSet};
use std::iter;

/// Verify writer panics if too large elements are inserted into a binary column of ColumnarRowSet
/// buffer.
#[test]
#[should_panic]
fn insert_too_large_element_in_bin_column() {
    // Fill buffer with values
    let desc = BufferDescription {
        kind: BufferKind::Binary { length: 1 },
        nullable: true,
    };
    let mut buffer = ColumnarRowSet::new(10, iter::once(desc));
    buffer.set_num_rows(1);
    if let AnyColumnViewMut::Binary(mut col) = buffer.column_mut(0) {
        col.write(iter::once(Some(&b"too large input."[..])))
    }
}

/// Verify writer panics if too large elements are inserted into a text column of ColumnarRowSet
/// buffer.
#[test]
#[should_panic]
fn insert_too_large_element_in_text_column() {
    // Fill buffer with values
    let desc = BufferDescription {
        kind: BufferKind::Text { max_str_len: 1 },
        nullable: true,
    };
    let mut buffer = ColumnarRowSet::new(10, iter::once(desc));
    buffer.set_num_rows(1);
    if let AnyColumnViewMut::Text(mut col) = buffer.column_mut(0) {
        col.write(iter::once(Some(&b"too large input."[..])))
    }
}

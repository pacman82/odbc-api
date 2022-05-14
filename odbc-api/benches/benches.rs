use std::iter;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use odbc_api::buffers::{BufferDescription, ColumnarAnyBuffer};

fn falliable_buffer_allocation(capacity: usize, max_str_len: usize) {
    let description = BufferDescription {
        kind: odbc_api::buffers::BufferKind::Text { max_str_len },
        nullable: true,
    };
    ColumnarAnyBuffer::try_from_description(capacity, iter::once(description)).unwrap();
}

fn infalliable_buffer_allocation(capacity: usize, max_str_len: usize) {
    let description = BufferDescription {
        kind: odbc_api::buffers::BufferKind::Text { max_str_len },
        nullable: true,
    };
    ColumnarAnyBuffer::from_description(capacity, iter::once(description));
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("fallibale buffer allocation", |b| {
        b.iter(|| {
            let capacity = 1000;
            let max_str_len = 65536;
            falliable_buffer_allocation(black_box(capacity), black_box(max_str_len))
        })
    });
    c.bench_function("infallibale buffer allocation", |b| {
        b.iter(|| {
            let capacity = 1000;
            let max_str_len = 65536;
            infalliable_buffer_allocation(black_box(capacity), black_box(max_str_len))
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

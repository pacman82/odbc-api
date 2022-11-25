use std::iter;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use odbc_api::buffers::{BufferDesc, ColumnarAnyBuffer};

fn falliable_buffer_allocation(capacity: usize, max_str_len: usize) {
    let description = BufferDesc::Text { max_str_len };
    ColumnarAnyBuffer::try_from_descs(capacity, iter::once(description)).unwrap();
}

fn infalliable_buffer_allocation(capacity: usize, max_str_len: usize) {
    let description = BufferDesc::Text { max_str_len };
    ColumnarAnyBuffer::from_descs(capacity, [description]);
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

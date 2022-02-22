use criterion::{criterion_group, criterion_main, Criterion};

fn bench_iter<'a>(iter: impl Iterator<Item = Option<&'a [u8]>>) {
    let len = iter.size_hint().0;
    let mut offsets = Vec::with_capacity(len + 1);
    offsets.push(0);
    let mut values = Vec::with_capacity(0);
    let mut validity = Vec::with_capacity(len);

    let mut length = 0;
    for item in iter {
        if let Some(inner) = item {
            length += inner.len();
            validity.push(true);
            values.extend_from_slice(inner)
        } else {
            validity.push(false);
        }
        offsets.push(length);
    }
}

fn bench_slices(slice: &[u8], indicators: &[isize], max_length: usize) {
    let len = indicators.len();
    let mut offsets = Vec::with_capacity(len + 1);
    offsets.push(0);
    let mut validity = Vec::with_capacity(len);

    let mut length = 0;
    let max_length_i = max_length as isize;
    offsets.extend(indicators.iter().map(|&indicator| {
        validity.push(indicator != -1);
        length += if indicator > 0 && indicator <= max_length_i {
            indicator as i32
        } else {
            0
        };
        length
    }));

    assert_eq!(slice.len(), max_length * indicators.len());
    let mut values = Vec::<u8>::with_capacity(length as usize);
    offsets.windows(2).enumerate().for_each(|(index, x)| {
        let len = (x[1] - x[0]) as usize;
        let offset = index * max_length;

        // this is proven because:
        // last offset == (indicators.len() - 1) * max_length
        // last length <= max_length;
        let slice = unsafe { slice.get_unchecked(offset..offset + len) };
        values.extend_from_slice(slice);
    });
}

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let num_rows = 2usize.pow(log2_size);
        let max_len = 40;
        // 1 every 10 is null
        let is_null = |x: usize| x % 10 == 0;

        let col = odbc_api::buffers::BinColumn {
            max_len,
            values: (0..max_len * num_rows).map(|x| (x % 255) as u8).collect(),
            indicators: (0..num_rows)
                .map(|x| {
                    if is_null(x) {
                        -1
                    } else {
                        (x % (max_len + 1)) as isize
                    }
                })
                .collect(),
        };

        c.bench_function(&format!("iter 2^{}", log2_size), |b| {
            b.iter(|| {
                let iter = odbc_api::buffers::BinColumnIt {
                    pos: 0,
                    num_rows,
                    col: &col,
                };
                bench_iter(iter);
            })
        });

        c.bench_function(&format!("slices 2^{}", log2_size), |b| {
            b.iter(|| {
                bench_slices(&col.values, &col.indicators, col.max_len);
            })
        });
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);

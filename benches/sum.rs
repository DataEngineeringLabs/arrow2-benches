use std::iter::FromIterator;

use criterion::*;

use rand::distributions::{Distribution, Standard};
use rand::{rngs::StdRng, Rng, SeedableRng};

use arrow2::array::PrimitiveArray;
use arrow2::compute::aggregate::sum_primitive;

fn sum(array: &[Option<i32>]) -> i32 {
    black_box(array.iter().flatten().sum())
}

use std::convert::TryInto;

const LANES: usize = 16;

pub fn nonsimd_sum(values: &[i32]) -> i32 {
    let chunks = values.chunks_exact(LANES);
    let remainder = chunks.remainder();

    let sum = chunks.fold([0; LANES], |mut acc, chunk| {
        let chunk: [i32; LANES] = chunk.try_into().unwrap();
        for i in 0..LANES {
            acc[i] += chunk[i];
        }
        acc
    });

    let remainder: i32 = remainder.iter().copied().sum();

    let mut reduced = 0;
    (0..LANES).for_each(|i| {
        reduced += sum[i];
    });
    reduced + remainder
}

fn create_array<T: FromIterator<Option<i32>>>(size: usize, null_density: f32) -> T
where
    Standard: Distribution<i32>,
{
    let mut rng = StdRng::seed_from_u64(42);

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                Some(rng.gen())
            }
        })
        .collect()
}

fn add_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("sum");

    for log2_size in (10..=20).step_by(2) {
        let size = 2usize.pow(log2_size);
        let arrow_array: PrimitiveArray<i32> = create_array(size, 0.1);
        let option_array: Vec<Option<i32>> = create_array(size, 0.1);
        let expected = sum(&option_array);

        group.bench_with_input(
            BenchmarkId::new("arrow null", log2_size),
            &arrow_array,
            |b, array| b.iter(|| assert!(sum_primitive(array).unwrap() == expected)),
        );

        group.bench_with_input(
            BenchmarkId::new("option null", log2_size),
            &option_array,
            |b, array| b.iter(|| assert!(sum(array) == expected)),
        );

        let arrow_array: PrimitiveArray<i32> = create_array(size, 0.0);
        let option_array: Vec<Option<i32>> = create_array(size, 0.0);
        let expected = sum(&option_array);

        group.bench_with_input(
            BenchmarkId::new("arrow", log2_size),
            &arrow_array,
            |b, array| b.iter(|| assert!(sum_primitive(array).unwrap() == expected)),
        );

        group.bench_with_input(
            BenchmarkId::new("option", log2_size),
            &option_array,
            |b, array| b.iter(|| assert!(sum(array) == expected)),
        );

        group.bench_with_input(
            BenchmarkId::new("native", log2_size),
            arrow_array.values().as_ref(),
            |b, array| b.iter(|| assert!(nonsimd_sum(array) == expected)),
        );

        group.throughput(Throughput::Elements(size as u64));
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);

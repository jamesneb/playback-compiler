use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use playback_compiler::transform::encode::encode_many_ids_arrow_bytes;
use rand::{distributions::Alphanumeric, Rng};

fn rand_id(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn bench_encode_many_ids(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_many_ids_arrow");
    for &n_ids in &[1usize, 8, 64, 512, 4096] {
        group.bench_with_input(BenchmarkId::from_parameter(n_ids), &n_ids, |b, &n| {
            // Prepare a fixed input set per iteration size
            let ids: Vec<Bytes> = (0..n)
                .map(|i| {
                    // vary length a little to avoid degenerate layouts
                    let s = rand_id(8 + (i % 17));
                    Bytes::from(s)
                })
                .collect();

            b.iter(|| {
                let out = encode_many_ids_arrow_bytes(&ids, false).expect("encode");
                criterion::black_box(out);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_encode_many_ids);
criterion_main!(benches);

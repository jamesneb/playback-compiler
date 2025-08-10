//! Bench: Arrow encode
//! Measures encode latency across varying id lengths for replay deltas.

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use playback_compiler::proto::Job;
use playback_compiler::transform::encode::encode_replay_delta_arrow;
use rand::{distributions::Alphanumeric, Rng};

fn random_id(n: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

fn bench_encode(c: &mut Criterion) {
    let mut g = c.benchmark_group("encode_replay_delta_arrow");
    for &len in &[8usize, 64, 512, 4096] {
        g.bench_with_input(format!("id_len_{len}"), &len, |b, &n| {
            b.iter_batched(
                || Job { id: random_id(n) },
                |job| {
                    let bytes = encode_replay_delta_arrow(&job).expect("encode");
                    black_box(bytes);
                },
                BatchSize::SmallInput,
            )
        });
    }
    g.finish();
}

criterion_group!(benches, bench_encode);
criterion_main!(benches);

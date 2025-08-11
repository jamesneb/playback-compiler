use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use playback_compiler::emit::{DeltaKeyBuilder, SimpleKeyBuilder};
use playback_compiler::proto::Job;
use playback_compiler::transform::encode::encode_many_ids_arrow_bytes;

/// Benchmarks key construction and Arrow encoding without external I/O.
fn bench_key_and_encode(c: &mut Criterion) {
    let kb = SimpleKeyBuilder::new("tenants/default");
    let job = Job {
        id: "job-xyz".into(),
    };

    c.bench_function("key_build", |b| {
        b.iter(|| {
            // Use fixed timestamps; actual values are irrelevant to throughput.
            let key = kb.replay_delta_key(&job, 1_712_345_678, 123);
            criterion::black_box(key);
        })
    });

    c.bench_function("encode_many_ids_arrow/1", |b| {
        let ids = vec![Bytes::from_static(b"job-xyz")];
        b.iter(|| {
            let out = encode_many_ids_arrow_bytes(&ids, false).expect("encode");
            criterion::black_box(out);
        });
    });
}

criterion_group!(benches, bench_key_and_encode);
criterion_main!(benches);

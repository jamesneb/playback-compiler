use async_trait::async_trait;
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use playback_compiler::emit::{emit_replay_delta, ReplaySink, SimpleKeyBuilder};
use playback_compiler::errors::CompilerError;
use playback_compiler::proto::Job;
use playback_compiler::transform::encode::encode_replay_delta_arrow;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

#[derive(Clone, Default)]
struct SinkCount(Arc<Mutex<usize>>);

#[async_trait]
impl ReplaySink for SinkCount {
    type Error = CompilerError;
    async fn put_delta(&self, _key: &str, _bytes: Bytes) -> Result<(), Self::Error> {
        *self.0.lock().unwrap() += 1;
        Ok(())
    }
}

fn bench_emit(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio rt");

    let sink = SinkCount::default();
    let kb = SimpleKeyBuilder::new("tenants/default");
    let job = Job { id: "bench".into() };
    let bytes = Bytes::from(encode_replay_delta_arrow(&job).expect("encode"));

    c.bench_function("emit_replay_delta_ok", |b| {
        b.iter(|| {
            rt.block_on(async {
                emit_replay_delta(&sink, &kb, &job, bytes.clone())
                    .await
                    .expect("emit");
                black_box(());
            })
        })
    });
}

criterion_group!(benches, bench_emit);
criterion_main!(benches);

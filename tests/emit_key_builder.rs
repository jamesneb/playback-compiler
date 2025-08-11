use playback_compiler::emit::{DeltaKeyBuilder, SimpleKeyBuilder};
use playback_compiler::proto::Job;

#[test]
fn simple_key_builder_shapes_name() {
    let kb = SimpleKeyBuilder::new("tenants/default");
    let job = Job { id: "j1".into() };
    let key = kb.replay_delta_key(&job, 1_712_345_678, 123);
    assert!(key.starts_with("tenants/default/replay-deltas/j1/"));
    assert!(key.ends_with(".arrow"));
    assert!(key.contains("1712345678-000000123"));
}

#[test]
fn simple_key_builder_allows_unicode_ids() {
    let kb = SimpleKeyBuilder::new("pfx");
    let job = Job {
        id: "服务-λ".into(),
    };
    let key = kb.replay_delta_key(&job, 100, 7);
    assert!(key.starts_with("pfx/replay-deltas/"));
    assert!(key.contains("100-000000007"));
    assert!(key.ends_with(".arrow"));
}

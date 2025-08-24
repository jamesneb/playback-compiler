//! Integration test exercising Redis Streams ingestion.

use bytes::Bytes;
use deadpool_redis::redis;
use testcontainers::core::WaitFor;
use testcontainers::{clients, GenericImage};

use playback_compiler::ingest::Queue;
use playback_compiler::redis::RedisStreamQueue;

#[tokio::test]
async fn redis_streams_end_to_end() {
    let docker = clients::Cli::default();
    let img = GenericImage::new("redis", "7-alpine")
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"));
    let node = docker.run(img);
    let port = node.get_host_port_ipv4(6379);
    let url = format!("redis://127.0.0.1:{port}");

    // Pop a message and acknowledge it.
    let q = RedisStreamQueue::from_url("jobs", "compilers", "c1", &url).expect("queue");
    q.ensure_stream_group().await.expect("stream group");

    {
        let mut conn = q.pool_clone().get().await.unwrap();
        let _: String = redis::cmd("XADD")
            .arg("jobs")
            .arg("*")
            .arg("payload")
            .arg(b"hello")
            .query_async(&mut *conn)
            .await
            .unwrap();
    }

    let msg = match q.pop().await.expect("pop") {
        Some(msg) => msg,
        None => panic!("no message"),
    };
    assert_eq!(msg.payload, Bytes::from_static(b"hello"));
    q.ack(&msg.id).await.expect("ack");

    {
        let mut conn = q.pool_clone().get().await.unwrap();
        let val: redis::Value = redis::cmd("XPENDING")
            .arg("jobs")
            .arg("compilers")
            .arg("-")
            .arg("+")
            .arg(10)
            .query_async(&mut *conn)
            .await
            .unwrap();
        match val {
            redis::Value::Bulk(v) => assert!(v.is_empty(), "expected no pending, got {v:?}"),
            redis::Value::Int(i) => assert_eq!(i, 0),
            other => panic!("unexpected XPENDING shape: {other:?}"),
        }
    }
}

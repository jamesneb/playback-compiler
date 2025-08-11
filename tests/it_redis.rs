//! Integration: Redis Streams

use bytes::Bytes;
use deadpool_redis::redis;
use testcontainers::core::WaitFor;
use testcontainers::{clients, GenericImage};

use playback_compiler::ingest::Queue;
use playback_compiler::redis::{init_redis_pool, pool, RedisStreamQueue};

#[tokio::test]
async fn redis_streams_end_to_end() {
    let docker = clients::Cli::default();
    let img = GenericImage::new("redis", "7-alpine")
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"));
    let node = docker.run(img);
    let port = node.get_host_port_ipv4(6379);
    let url = format!("redis://127.0.0.1:{port}");

    init_redis_pool(&url).await.unwrap();

    // pop/ack
    let q = RedisStreamQueue::new(pool().clone(), "jobs", "compilers", "c1");
    q.ensure_stream_group().await.unwrap();

    {
        let mut conn = pool().get().await.unwrap();
        let _: String = redis::cmd("XADD")
            .arg("jobs")
            .arg("*")
            .arg("payload")
            .arg(b"hello")
            .query_async(&mut *conn)
            .await
            .unwrap();
    }

    let msg = q.pop().await.unwrap().expect("expected one message");
    assert_eq!(msg.payload, Bytes::from_static(b"hello"));
    q.ack(&msg.id).await.unwrap();

    {
        let mut conn = pool().get().await.unwrap();
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
            redis::Value::Bulk(v) => assert!(v.is_empty(), "expected no pending, got {:?}", v),
            redis::Value::Int(i) => assert_eq!(i, 0),
            other => panic!("unexpected XPENDING shape: {:?}", other),
        }
    }
}

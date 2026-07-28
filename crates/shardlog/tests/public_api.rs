use bytes::Bytes;
use shardlog::{ShardLog, ShardLogConfig};

#[test]
fn public_log_api_survives_durable_reopen_and_compaction() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let config = ShardLogConfig::new(temporary.path().join("log"));

    let mut log = ShardLog::open(config.clone()).expect("open log");
    let receipt = log
        .append_group(
            0,
            &[Bytes::from_static(b"first"), Bytes::from_static(b"second")],
            true,
        )
        .expect("durable append");
    assert_eq!(receipt.first_sequence, 0);
    assert_eq!(receipt.last_sequence, 1);
    assert_eq!(log.bounds().expect("bounds"), Some((0, 2)));
    drop(log);

    let mut reopened = ShardLog::open(config).expect("reopen log");
    let records = reopened.read_from(0, 1024, 8).expect("read records");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload.as_ref(), b"first");
    assert_eq!(records[1].payload.as_ref(), b"second");
    reopened.compact_before(1).expect("compact sealed prefix");
}

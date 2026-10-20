//! Public native durability API against a genuine appended and reopened log.

use bytes::Bytes;
use shardlog::{
    ShardLog, ShardLogConfig, StorageDurabilityBudget, StorageDurabilityLimits,
    storage_durability_cost,
};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Root(PathBuf);
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn public_sync_until_keeps_same_native_log_and_records_actual_flush_sync_before_reopen() {
    let root = Root(std::env::temp_dir().join(
        format!("shardlog-public-sync-{}-{}", std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos()),
    ));
    let config = ShardLogConfig::new(&root.0);
    let mut log = ShardLog::open(config.clone()).expect("actual native log");
    log.append_group(0, &[Bytes::from_static(b"actual native payload")], false)
        .expect("real ordinary append before additive native sync");
    let limits = StorageDurabilityLimits { max_io_calls: 2 };
    let cost = storage_durability_cost(limits).expect("actual checked public cost");
    assert!(cost.declared_bytes > 0 && cost.entries >= 2);
    let original = Instant::now() + Duration::from_secs(30);
    let mut budget =
        StorageDurabilityBudget::new(limits, original).expect("prepaid finite native call slots");
    log.sync_until(&mut budget)
        .expect("actual native flush and data sync under original E");
    assert_eq!(budget.absolute_expiry(), original);
    assert_eq!(budget.outcomes().len(), 2);
    assert!(
        budget
            .outcomes()
            .iter()
            .all(|row| row.entered() && matches!(row.result(), Some(Ok(()))))
    );
    assert!(
        ShardLog::open(config.clone()).is_err(),
        "same physical lease still belongs to log"
    );
    drop(log);
    let mut reopened = ShardLog::open(config).expect("actual native reopen");
    let records = reopened
        .read_from(0, 1024, 1)
        .expect("actual native disk read");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].sequence, 0);
    assert_eq!(records[0].payload.as_ref(), b"actual native payload");
}

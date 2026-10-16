//! Genuine native interrupted SSE8 production and same-attempt admission cases.
use super::*;
use shardlog::{RECOVERY_EXTENT_HEADER_BYTES, RecoveryExtentTailUnknown};

fn interrupted_maximum(root: &Path, keep: Option<usize>) -> (PathBuf, u64, Vec<u8>) {
    let path = write_native(root);
    let start = fs::metadata(&path)
        .expect("actual complete earlier frames")
        .len();
    let mut store = ShardStore::open(store_config(root)).expect("actual native writer reopen");
    store
        .append(
            reservation(2, u64::MAX),
            None,
            b"real final MAX term payload",
            true,
        )
        .expect("actual native MAX reservation writer");
    drop(store);
    let complete = fs::read(&path).expect("actual native complete MAX frame");
    let keep = keep.unwrap_or(complete.len() - start as usize - 1);
    let length = start + keep as u64;
    OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("actual native file")
        .set_len(length)
        .expect("physical interruption before final checksum settles");
    (path, start, complete[..length as usize].to_vec())
}

#[test]
fn actual_native_maximum_header_survives_missing_checksum_and_pending_name() {
    let temp = Temporary::new();
    let (pack, start, original) = interrupted_maximum(&temp.0, None);
    let pending = pack.with_extension("sse.gc-pending");
    fs::rename(pack, &pending).expect("native pending deletion provenance");
    let owner = DirectoryOwnership::acquire(&temp.0).expect("real OS owner");
    let census = NativeRecoveryCensus::read(
        config(&temp.0),
        Arc::clone(&owner),
        None,
        limits(),
        Instant::now() + Duration::from_secs(30),
    )
    .expect("no mutation or repair while deriving header");
    assert!(Arc::ptr_eq(census.directory_ownership(), &owner));
    let tails = census.extent_tails().collect::<Vec<_>>();
    assert_eq!(tails.len(), 1);
    let tail = tails[0];
    assert_eq!(tail.file_offset, start);
    assert_eq!(tail.physical_shard, ShardId::new(0));
    let RecoveryExtentTail::Header(header) = &tail.native else {
        panic!("actual complete fixed header must retain MAX");
    };
    assert_eq!(header.reservation, reservation(2, u64::MAX));
    assert_eq!(
        header
            .reservation
            .placement
            .leader_epoch
            .get()
            .checked_add(1),
        None
    );
    let source = census
        .sources()
        .nth(tail.source_index)
        .expect("real exact source index");
    assert_eq!(source.kind, RecoverySourceKind::PendingDeletion);
    assert_eq!(source.path, pending);
    assert_eq!(source.partial_tail, original[start as usize..]);
    assert_eq!(header.declared_frame_bytes, source.partial_tail.len() + 1);
    assert_eq!(
        census.extents().len(),
        2,
        "incomplete header is NEVER a complete extent"
    );
    assert_eq!(
        census.observed_leader_epoch(TopicPartition::new(
            TopicId::new(11),
            LogicalPartitionId::new(0)
        )),
        Some(u64::MAX)
    );
    assert_eq!(fs::read(&pending).expect("raw original evidence"), original);
    assert!(matches!(
        DirectoryOwnership::acquire(&temp.0),
        Err(DirectoryOwnershipError::Contended)
    ));
}

#[test]
fn actual_short_or_uninterpretable_native_tail_stays_explicitly_unknown() {
    for invalid in [false, true] {
        let temp = Temporary::new();
        let keep = if invalid {
            RECOVERY_EXTENT_HEADER_BYTES
        } else {
            RECOVERY_EXTENT_HEADER_BYTES - 1
        };
        let (path, start, mut original) = interrupted_maximum(&temp.0, Some(keep));
        if invalid {
            // Corrupt the real native payload-length field after interruption.
            // No checksum is fabricated and this is not a valid extent claim.
            original[start as usize + RECOVERY_EXTENT_HEADER_BYTES - 8] ^= 1;
            fs::write(&path, &original).expect("actual uninterpretable partial native header");
        }
        let census = read(&temp.0, config(&temp.0), limits());
        let tail = census
            .extent_tails()
            .next()
            .expect("unknown is retained, never dropped");
        assert_eq!(tail.file_offset, start);
        assert_eq!(
            tail.native,
            RecoveryExtentTail::Unknown(if invalid {
                RecoveryExtentTailUnknown::InvalidHeader
            } else {
                RecoveryExtentTailUnknown::ShortHeader
            })
        );
        assert_eq!(
            census
                .sources()
                .nth(tail.source_index)
                .expect("source")
                .partial_tail,
            original[start as usize..]
        );
        // This known numeric maximum is not evidence that unknown bytes are safe.
        assert_eq!(
            census.observed_leader_epoch(TopicPartition::new(
                TopicId::new(11),
                LogicalPartitionId::new(0)
            )),
            Some(91)
        );
        assert_eq!(fs::read(&path).expect("never repaired"), original);
    }
}

fn repeat_actual_tail(census: &mut NativeRecoveryCensus, source: usize, offset: u64) -> Result<()> {
    let file = census.attempt.sources[source]
        .handle
        .file()
        .try_clone()
        .expect("same retained native file");
    let bytes = census.attempt.sources[source].fact.bytes;
    let kind = census.attempt.sources[source].fact.kind;
    census
        .attempt
        .tail(source, &file, offset, bytes, kind, ShardId::new(0))
}

#[test]
fn actual_header_inspection_exact_and_one_less_original_allowances_precede_parser() {
    let temp = Temporary::new();
    let (path, start, original) = interrupted_maximum(&temp.0, None);
    let mut first = read(&temp.0, config(&temp.0), limits());
    let source = first
        .extent_tails()
        .next()
        .expect("actual source")
        .source_index;
    let before = first.usage();
    let calls = TAIL_HEADER_INSPECTIONS.with(std::cell::Cell::get);
    repeat_actual_tail(&mut first, source, start).expect("measure actual same-attempt repeat");
    assert_eq!(
        TAIL_HEADER_INSPECTIONS.with(std::cell::Cell::get),
        calls + 1
    );
    let measured = first.usage();
    let delta = NativeRecoveryUsage {
        files: measured.files - before.files,
        entries: measured.entries - before.entries,
        declared_bytes: measured.declared_bytes - before.declared_bytes,
        read_bytes: measured.read_bytes - before.read_bytes,
        work_bytes: measured.work_bytes - before.work_bytes,
    };
    assert_eq!(
        delta.files, 0,
        "tail borrows the actual retained descriptor"
    );
    drop(first);
    for short in 0..5 {
        let mut census = read(&temp.0, config(&temp.0), limits());
        let source = census
            .extent_tails()
            .next()
            .expect("real tail")
            .source_index;
        let before = census.usage();
        census.attempt.limits.max_entries = before.entries + delta.entries;
        census.attempt.limits.max_declared_bytes = before.declared_bytes + delta.declared_bytes;
        census.attempt.limits.max_read_bytes = before.read_bytes + delta.read_bytes;
        census.attempt.limits.max_work_bytes = before.work_bytes + delta.work_bytes;
        match short {
            1 => census.attempt.limits.max_entries -= 1,
            2 => census.attempt.limits.max_declared_bytes -= 1,
            3 => census.attempt.limits.max_read_bytes -= 1,
            4 => census.attempt.limits.max_work_bytes -= 1,
            _ => {}
        }
        let calls = TAIL_HEADER_INSPECTIONS.with(std::cell::Cell::get);
        let result = repeat_actual_tail(&mut census, source, start);
        if short == 0 {
            result.expect("exact measured additional admission");
            assert_eq!(
                TAIL_HEADER_INSPECTIONS.with(std::cell::Cell::get),
                calls + 1
            );
        } else {
            assert!(result.is_err(), "one-less allowance must refuse");
            assert_eq!(
                TAIL_HEADER_INSPECTIONS.with(std::cell::Cell::get),
                calls,
                "no native header parser before debit"
            );
        }
        assert_eq!(
            census.sources().nth(source).expect("source").partial_tail,
            original[start as usize..]
        );
        assert_eq!(
            fs::read(&path).expect("no mutation under any cap"),
            original
        );
    }
    let mut caps = limits();
    caps.max_partial_tail_bytes = original.len() - start as usize - 1;
    let owner = DirectoryOwnership::acquire(&temp.0).expect("real owner after settled refusals");
    assert!(
        NativeRecoveryCensus::read(
            config(&temp.0),
            owner,
            None,
            caps,
            Instant::now() + Duration::from_secs(30)
        )
        .is_err()
    );
    assert_eq!(fs::read(path).expect("no tail truncation"), original);
}

#[test]
fn real_expired_original_tail_attempt_does_not_read_or_inspect_again() {
    let temp = Temporary::new();
    let (path, start, original) = interrupted_maximum(&temp.0, None);
    let owner = DirectoryOwnership::acquire(&temp.0).expect("actual original OS custody");
    let expiry = Instant::now() + Duration::from_secs(2);
    let mut census = NativeRecoveryCensus::read(config(&temp.0), owner, None, limits(), expiry)
        .expect("real read inside original E");
    let source = census
        .extent_tails()
        .next()
        .expect("real source")
        .source_index;
    let used = census.usage();
    let calls = TAIL_HEADER_INSPECTIONS.with(std::cell::Cell::get);
    std::thread::sleep(expiry.saturating_duration_since(Instant::now()) + Duration::from_millis(5));
    assert!(repeat_actual_tail(&mut census, source, start).is_err());
    assert_eq!(census.absolute_expiry(), expiry);
    assert_eq!(census.usage().read_bytes, used.read_bytes);
    assert_eq!(TAIL_HEADER_INSPECTIONS.with(std::cell::Cell::get), calls);
    assert!(matches!(
        DirectoryOwnership::acquire(&temp.0),
        Err(DirectoryOwnershipError::Contended)
    ));
    assert_eq!(
        fs::read(&path).expect("original raw bytes unchanged"),
        original
    );
    drop(census);
    assert!(DirectoryOwnership::acquire(&temp.0).is_ok());
}

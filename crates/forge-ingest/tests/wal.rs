#![allow(clippy::print_stderr, clippy::print_stdout)]
mod common;
use common::*;
use forge_ingest::wal::{self, REC_HDR, Wal, WalError, WalOptions, list_segments, lock_dir, replay, truncate};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::{Arc, Mutex};

fn opts(dir: &std::path::Path, seg: u64) -> WalOptions {
    WalOptions {
        segment_bytes: seg,
        ..WalOptions::new(dir.to_path_buf())
    }
}

/// `encode` for tests whose items are already the payload.
fn take(p: &mut Vec<u8>, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&std::mem::take(p))
}

fn payload(i: u64) -> Vec<u8> {
    let mut r = Rng(i + 1);
    wal::encode_batch(&batch_from_rows(&[random_row(&mut r, "p", i, i, i + 1)])).unwrap()
}

/// Writes `n` records from 32 concurrent tasks; returns (dir records in on_durable order, stats).
async fn fill(dir: &std::path::Path, next_seq: u64, n: u64, seg: u64) -> (Vec<u64>, wal::WriterStats) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s2 = seen.clone();
    let _lock = lock_dir(dir).unwrap();
    let w = Arc::new(
        Wal::start(
            opts(dir, seg),
            next_seq,
            take,
            move |items: &mut Vec<(u64, Vec<u8>)>| {
                s2.lock().unwrap().extend(items.drain(..).map(|(seq, _)| seq));
            },
        )
        .unwrap(),
    );
    let mut tasks = Vec::new();
    for t in 0..32 {
        let w = w.clone();
        tasks.push(tokio::spawn(async move {
            let mut acks = Vec::new();
            for i in (t..n).step_by(32) {
                acks.push(w.append(payload(i)).await.unwrap());
            }
            acks
        }));
    }
    let mut acked = Vec::new();
    for t in tasks {
        acked.extend(t.await.unwrap());
    }
    let stats = tokio::task::block_in_place(|| w.shutdown());
    acked.sort();
    assert_eq!(
        acked,
        (next_seq..next_seq + n).collect::<Vec<_>>(),
        "acks = contiguous seqs"
    );
    let seen = seen.lock().unwrap().clone();
    assert!(seen.windows(2).all(|w| w[1] == w[0] + 1), "on_durable sees seq order");
    (seen, stats)
}

fn replay_all(dir: &std::path::Path, flushed: u64) -> Result<(Vec<u64>, wal::ReplayStats), WalError> {
    let mut seqs = Vec::new();
    let st = replay(dir, flushed, |seq, p| {
        wal::decode_batch(p).map(|b| assert_eq!(b.num_rows(), 1))?;
        seqs.push(seq);
        Ok::<_, arrow::error::ArrowError>(())
    })?;
    Ok((seqs, st))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_commit_rotation_replay_truncate() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("node-1");
    let (_, stats) = fill(&dir, 1, 2000, 64 << 10).await;
    eprintln!("group commit: {stats:?}");
    assert!(stats.fsyncs < stats.records, "records share fsyncs");
    assert!(list_segments(&dir).unwrap().len() > 5, "rotated");

    let (seqs, st) = replay_all(&dir, 0).unwrap();
    assert_eq!(seqs, (1..=2000).collect::<Vec<_>>());
    assert_eq!((st.next_seq, st.truncated.clone()), (2001, None));

    // Watermark 1234: delete only segments entirely <= 1234; replay(1234) yields exactly 1235..=2000.
    let segs = list_segments(&dir).unwrap();
    let deleted = truncate(&dir, 1234).unwrap();
    let left = list_segments(&dir).unwrap();
    assert_eq!(segs.len() - deleted, left.len());
    assert!(left[0].0 <= 1235, "segment holding 1235 kept");
    let (seqs, st) = replay_all(&dir, 1234).unwrap();
    assert_eq!(seqs, (1235..=2000).collect::<Vec<_>>());
    assert_eq!(st.next_seq, 2001);
    // Everything flushed: all but the newest file go; the newest is never deleted.
    truncate(&dir, 2000).unwrap();
    assert_eq!(list_segments(&dir).unwrap().len(), 1);
    // Restart at next_seq (the newest file may be an empty, just-rotated one with that name).
    let (seen, _) = fill(&dir, 2001, 10, 64 << 10).await;
    assert_eq!(seen, (2001..=2010).collect::<Vec<_>>());
    assert_eq!(replay_all(&dir, 2000).unwrap().0, (2001..=2010).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn torn_tails_are_cut_exactly() {
    type Corrupt = fn(&mut Vec<u8>);
    let cases: [(&str, Corrupt); 6] = [
        ("partial header", |v| v.extend_from_slice(&[0xFF; 9])),
        ("header only, length past EOF", |v| {
            v.extend_from_slice(&1000u32.to_le_bytes());
            v.extend_from_slice(&[0; 12]);
        }),
        ("full record, bad crc", |v| {
            v.extend_from_slice(&10u32.to_le_bytes());
            v.extend_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
            v.extend_from_slice(&999u64.to_le_bytes());
            v.extend_from_slice(&[7; 10]);
        }),
        ("zero-filled tail (delayed allocation)", |v| {
            v.extend_from_slice(&[0; 4096])
        }),
        ("half of a real record", |v| {
            let mut rec = Vec::new();
            let p = payload(77);
            rec.extend_from_slice(&(p.len() as u32).to_le_bytes());
            rec.extend_from_slice(&[1, 2, 3, 4]);
            rec.extend_from_slice(&301u64.to_le_bytes());
            rec.extend_from_slice(&p);
            v.extend_from_slice(&rec[..rec.len() / 2]);
        }),
        ("short tail < header", |v| v.extend_from_slice(&[1, 2, 3])),
    ];
    for (name, corrupt) in cases {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("n");
        fill(&dir, 1, 300, 16 << 10).await;
        let (_, last) = list_segments(&dir).unwrap().pop().unwrap();
        // the newest file may be a just-rotated empty one; corrupt the newest non-empty is not needed:
        let good_len = fs::metadata(&last).unwrap().len();
        let mut tail = Vec::new();
        corrupt(&mut tail);
        OpenOptions::new()
            .append(true)
            .open(&last)
            .unwrap()
            .write_all(&tail)
            .unwrap();
        let (seqs, st) = replay_all(&dir, 0).unwrap();
        assert_eq!(seqs, (1..=300).collect::<Vec<_>>(), "{name}");
        assert_eq!(st.truncated.as_ref().map(|t| t.1), Some(good_len), "{name}");
        assert_eq!(
            fs::metadata(&last).unwrap().len(),
            good_len,
            "{name}: cut back to the last good record"
        );
        // idempotent: a second replay finds nothing to cut, and appends continue at 301
        assert_eq!(replay_all(&dir, 0).unwrap().1.truncated, None, "{name}");
        let (seen, _) = fill(&dir, st.next_seq, 5, 16 << 10).await;
        assert_eq!(seen, (301..=305).collect::<Vec<_>>());
        assert_eq!(replay_all(&dir, 0).unwrap().0.len(), 305, "{name}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sealed_corruption_and_bad_seq_are_hard_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("n");
    fill(&dir, 1, 300, 16 << 10).await;
    let segs = list_segments(&dir).unwrap();
    // flip a payload byte in the first (sealed) segment
    let mut bytes = fs::read(&segs[0].1).unwrap();
    bytes[8 + REC_HDR + 3] ^= 0xFF;
    fs::write(&segs[0].1, &bytes).unwrap();
    assert!(matches!(replay_all(&dir, 0), Err(WalError::Corrupt { .. })));
    // A callback error is surfaced with its seq (e.g. a batch that fails validate_batch).
    let dir2 = tmp.path().join("m");
    fill(&dir2, 1, 3, 1 << 20).await;
    let e = replay(&dir2, 0, |seq, _| if seq == 2 { Err("bad batch") } else { Ok(()) }).unwrap_err();
    assert!(matches!(e, WalError::Apply { seq: 2, .. }), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_during_rotation_leaves_empty_newest_file() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("n");
    fill(&dir, 1, 20, 1 << 20).await;
    // a later segment created but its magic never made it to disk
    fs::write(wal::segment_path(&dir, 21), b"FOR").unwrap();
    let (seqs, st) = replay_all(&dir, 0).unwrap();
    assert_eq!((seqs.len(), st.next_seq, st.removed_empty), (20, 21, 1));
    // a full-magic empty newest file: reused by name on restart
    fs::write(wal::segment_path(&dir, 21), b"FORGWAL1").unwrap();
    let st = replay_all(&dir, 0).unwrap().1;
    let (seen, _) = fill(&dir, st.next_seq, 3, 1 << 20).await;
    assert_eq!(seen, vec![21, 22, 23]);
}

#[test]
fn second_process_cannot_lock() {
    let tmp = tempfile::tempdir().unwrap();
    let _a = lock_dir(tmp.path()).unwrap();
    assert!(matches!(lock_dir(tmp.path()), Err(WalError::Locked(_))));
}

#[test]
fn ipc_decoded_batches_overcount_memory() {
    // Why the memtable accounts WAL payload bytes, not get_array_memory_size().
    let mut r = Rng(3);
    let rows: Vec<_> = (0..100).map(|i| random_row(&mut r, "p", i, i, i + 1)).collect();
    let b = batch_from_rows(&rows);
    let p = wal::encode_batch(&b).unwrap();
    let d = wal::decode_batch(&p).unwrap();
    eprintln!(
        "100 rows: built={} B, ipc payload={} B, decoded get_array_memory_size={} B",
        b.get_array_memory_size(),
        p.len(),
        d.get_array_memory_size()
    );
    let slice = |b: &arrow::array::RecordBatch| -> usize {
        b.columns()
            .iter()
            .map(|c| c.to_data().get_slice_memory_size().unwrap())
            .sum()
    };
    eprintln!("get_slice_memory_size: built={} B, decoded={} B", slice(&b), slice(&d));
    assert_eq!(slice(&b), slice(&d));
    let one = wal::encode_batch(&batch_from_rows(&rows[..1])).unwrap();
    eprintln!(
        "1-row IPC payload = {} B (schema message repeated per record)",
        one.len()
    );
    assert!(Arc::ptr_eq(&d.schema(), &forge_model::schema::spans_schema()));
}

// ---------- real crash: re-exec this test binary as a child that aborts mid-write ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_writer() {
    let Ok(dir) = std::env::var("P2_CHILD_DIR") else { return };
    let dir = std::path::PathBuf::from(dir);
    let st = replay(&dir, 0, |_, _| Ok::<_, String>(())).unwrap();
    let _lock = lock_dir(&dir).unwrap();
    let w = Arc::new(
        Wal::start(
            opts(&dir, 32 << 10),
            st.next_seq,
            take,
            |_: &mut Vec<(u64, Vec<u8>)>| {},
        )
        .unwrap(),
    );
    let out = Arc::new(Mutex::new(std::io::stdout()));
    let mut tasks = Vec::new();
    for t in 0..16u64 {
        let (w, out) = (w.clone(), out.clone());
        tasks.push(tokio::spawn(async move {
            let mut i = 0;
            loop {
                i += 1;
                let seq = w.append(payload(t * 100_000 + i)).await.unwrap();
                let mut o = out.lock().unwrap();
                writeln!(o, "ACK {seq}").unwrap();
                o.flush().unwrap();
            }
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
}

#[test]
fn abort_mid_write_loses_no_acked_record() {
    let exe = std::env::current_exe().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("n");
    let mut acked_total = std::collections::BTreeSet::new();
    // Crashes at different points, same WAL dir: torn write, lost unsynced bytes, before fsync, after fsync
    // (pre-ack).
    for (fp, nth) in [
        ("wal_torn_write", 40),
        ("wal_lose_unsynced", 20),
        ("wal_before_fsync", 25),
        ("wal_after_fsync", 30),
    ] {
        let out = std::process::Command::new(&exe)
            .args(["child_writer", "--exact", "--nocapture", "--test-threads=1"])
            .env("P2_CHILD_DIR", &dir)
            .env("FORGE_FAILPOINT", format!("{fp}@{nth}"))
            .output()
            .unwrap();
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&out.status),
            Some(9),
            "child must be SIGKILLed: {:?}",
            out.status
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains(&format!("FORGE_FAILPOINT {fp} hit")), "{stderr}");
        let acked: Vec<u64> = String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|l| l.strip_prefix("ACK ")?.parse().ok())
            .collect();
        acked_total.extend(acked.iter().copied());
        let (seqs, st) = replay_all(&dir, 0).unwrap();
        let present: std::collections::BTreeSet<u64> = seqs.iter().copied().collect();
        let missing: Vec<_> = acked_total.difference(&present).collect();
        eprintln!(
            "{fp}@{nth}: acked {} this run, replayed {} total, truncated {:?}, missing acked: {missing:?}",
            acked.len(),
            seqs.len(),
            st.truncated.as_ref().map(|t| t.1)
        );
        assert!(missing.is_empty());
        if fp == "wal_torn_write" {
            assert!(st.truncated.is_some(), "the torn write cuts inside a record");
        }
        assert!(seqs.windows(2).all(|w| w[1] == w[0] + 1), "contiguous after crash");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn encode_runs_in_seq_order() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("n");
    let encoded = Arc::new(Mutex::new(Vec::new()));
    let e2 = encoded.clone();
    let _lock = lock_dir(&dir).unwrap();
    let encode = move |p: &mut Vec<u8>, buf: &mut Vec<u8>| {
        e2.lock().unwrap().push(p[0]);
        buf.extend_from_slice(&std::mem::take(p))
    };
    let w = Arc::new(Wal::start(opts(&dir, 1 << 20), 1, encode, |_: &mut Vec<(u64, Vec<u8>)>| {}).unwrap());
    let mut tasks = Vec::new();
    for t in 0..16u8 {
        let w = w.clone();
        tasks.push(tokio::spawn(async move {
            let mut acks = Vec::new();
            for _ in 0..20 {
                acks.push((w.append(vec![t]).await.unwrap(), t));
            }
            acks
        }));
    }
    let mut acks = Vec::new();
    for t in tasks {
        acks.extend(t.await.unwrap());
    }
    tokio::task::block_in_place(|| w.shutdown());
    acks.sort();
    let by_seq: Vec<u8> = acks.iter().map(|a| a.1).collect();
    assert_eq!(
        *encoded.lock().unwrap(),
        by_seq,
        "encode sees the items in ack seq order"
    );
}

/// The WAL thread stamps versions into a payload encoded with version 0; the bytes must equal encoding the
/// stamped batch (the old path), for any rows.
#[test]
fn stamped_payload_equals_encoding_the_stamped_batch() {
    let mut r = Rng(11);
    for n in [1u64, 2, 7, 100, 513] {
        let mut rows: Vec<_> = (0..n).map(|i| random_row(&mut r, "p", i % 5, i, 0)).collect();
        let mut payload = wal::encode_batch(&batch_from_rows(&rows)).unwrap();
        let at = wal::version_offset(&payload, n as usize).unwrap();
        let first = 1_000_000 + n * 3;
        wal::stamp_versions(&mut payload, at, first..first + n);
        for (i, row) in rows.iter_mut().enumerate() {
            row.version = first + i as u64;
        }
        assert_eq!(payload, wal::encode_batch(&batch_from_rows(&rows)).unwrap(), "{n} rows");
    }
}

/// `version_offset` is pinned to `spans_schema`: the 8-byte values it points at are exactly the version column,
/// for a batch whose other Int64 columns hold other non-zero values. A schema edit that moves buffers fails here.
#[test]
fn version_offset_points_at_the_version_column() {
    let mut r = Rng(17);
    let rows: Vec<_> = (0..50u64)
        .map(|i| {
            let mut row = random_row(&mut r, "p", i % 4, i, 7_000 + i);
            row.prompt_tokens = Some(1_000 + i as i64);
            row.completion_tokens = Some(2_000 + i as i64);
            row.total_tokens = Some(3_000 + i as i64);
            row
        })
        .collect();
    let (payload, at) = wal::encode_payload(&batch_from_rows(&rows)).unwrap();
    let got: Vec<u64> = payload[at..at + 8 * rows.len()]
        .as_chunks::<8>()
        .0
        .iter()
        .map(|c| i64::from_le_bytes(*c) as u64)
        .collect();
    assert_eq!(got, (7_000..7_050).collect::<Vec<_>>());
}

/// A payload that cannot be encoded or located is a Forge bug: Internal (500), never the client's 400.
#[test]
fn encode_failure_is_internal() {
    use forge_model::error::{Classify, ErrorKind};
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
        "x",
        arrow::datatypes::DataType::Int64,
        false,
    )]));
    let col: arrow::array::ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1, 2]));
    let not_spans = arrow::array::RecordBatch::try_new(schema, vec![col]).unwrap();
    let e = wal::encode_payload(&not_spans).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Internal, "{e}");
}

//! The Parquet bytes of ten writer shapes, pinned to the digests the v0.32.2 tag produced.

use std::sync::{Arc, Mutex};

use arrow::array::{
    ArrayRef, Decimal128Array, FixedSizeBinaryArray, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use rivet::config::CompressionType;
use rivet::format::Format;
use rivet::format::parquet::ParquetFormat;

const DIGESTS: &str = "tests/fixtures/parquet_bytes_0_32_2.txt";
const ROWS: usize = 250_000;
const ODD: [usize; 9] = [777, 25_000, 1, 40_000, 3, 19_999, 20_001, 50_000, 9_000];

struct Shared(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Shared {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn wide(lo: i64, n: usize, nullable: bool, nulls: bool) -> RecordBatch {
    let ids = lo..lo + n as i64;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, nullable),
        Field::new("v", DataType::Int64, nullable),
        Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            nullable,
        ),
        Field::new("s", DataType::Utf8, nullable),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter(ids.clone().map(Some))),
        Arc::new(Int64Array::from_iter(
            ids.clone()
                .map(|i| (!(nulls && i % 7 == 0)).then_some(i * 3)),
        )),
        Arc::new(TimestampMicrosecondArray::from_iter(
            ids.clone().map(|i| Some(i * 1_000_003)),
        )),
        Arc::new(StringArray::from_iter(ids.map(|i| {
            (!(nulls && i % 11 == 0)).then(|| format!("v{}", i % 40))
        }))),
    ];
    RecordBatch::try_new(schema, cols).unwrap()
}

fn fixed(lo: i64, n: usize, nulls: bool) -> RecordBatch {
    let ids = || (lo..lo + n as i64).map(i128::from);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("d19", DataType::Decimal128(19, 0), true),
        Field::new("d38", DataType::Decimal128(38, 2), true),
        Field::new("u", DataType::FixedSizeBinary(16), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(lo..lo + n as i64)),
            Arc::new(
                Decimal128Array::from_iter(ids().map(Some))
                    .with_precision_and_scale(19, 0)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from_iter(
                    ids().map(|i| (!(nulls && i % 5 == 0)).then_some(i * 7)),
                )
                .with_precision_and_scale(38, 2)
                .unwrap(),
            ),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    ids().map(|i| (i.wrapping_mul(0x9E37_79B9) as u128).to_le_bytes()),
                )
                .unwrap(),
            ),
        ],
    )
    .unwrap()
}

fn series(sizes: &[usize], make: &dyn Fn(i64, usize) -> RecordBatch) -> Vec<RecordBatch> {
    let mut lo = 1i64;
    sizes
        .iter()
        .map(|&n| {
            let batch = make(lo, n);
            lo += n as i64;
            batch
        })
        .collect()
}

type Shape = (
    &'static str,
    Vec<RecordBatch>,
    Option<usize>,
    CompressionType,
);

fn shapes() -> Vec<Shape> {
    let even = vec![10_000usize; ROWS / 10_000];
    let zstd = CompressionType::Zstd;
    vec![
        (
            "nullable_no_nulls/even",
            series(&even, &|l, n| wide(l, n, true, false)),
            None,
            zstd,
        ),
        (
            "nullable_no_nulls/odd",
            series(&ODD, &|l, n| wide(l, n, true, false)),
            None,
            zstd,
        ),
        (
            "non_nullable/even",
            series(&even, &|l, n| wide(l, n, false, false)),
            None,
            zstd,
        ),
        (
            "with_nulls/even",
            series(&even, &|l, n| wide(l, n, true, true)),
            None,
            zstd,
        ),
        (
            "with_nulls/odd",
            series(&ODD, &|l, n| wide(l, n, true, true)),
            None,
            zstd,
        ),
        (
            "fixed_width_no_nulls/even",
            series(&even, &|l, n| fixed(l, n, false)),
            None,
            zstd,
        ),
        (
            "fixed_width_nulls/even",
            series(&even, &|l, n| fixed(l, n, true)),
            None,
            zstd,
        ),
        (
            "dictionary_fallback/groups_45000_uncompressed",
            series(&even, &|l, n| wide(l, n, true, false)),
            Some(45_000),
            CompressionType::None,
        ),
        (
            "one_big_batch/no_nulls",
            series(&[ROWS], &|l, n| wide(l, n, true, false)),
            None,
            zstd,
        ),
        (
            "one_big_batch/nulls",
            series(&[ROWS], &|l, n| wide(l, n, true, true)),
            None,
            zstd,
        ),
    ]
}

fn file_bytes(shape: &Shape) -> Vec<u8> {
    let (_, batches, groups, codec) = shape;
    let out = Arc::new(Mutex::new(Vec::new()));
    let fmt = ParquetFormat::new(*codec, None, *groups);
    let mut writer = fmt
        .create_writer(&batches[0].schema(), Box::new(Shared(out.clone())))
        .unwrap();
    for batch in batches {
        writer.write_batch(batch).unwrap();
    }
    writer.finish().unwrap();
    out.lock().unwrap().clone()
}

fn digest_line(shape: &Shape) -> String {
    let bytes = file_bytes(shape);
    format!(
        "{} {} {:016x}",
        shape.0,
        bytes.len(),
        xxhash_rust::xxh3::xxh3_64(&bytes)
    )
}

fn digests_path() -> String {
    format!("{}/{DIGESTS}", env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn parquet_bytes_match_the_v0_32_2_digests() {
    let pinned = std::fs::read_to_string(digests_path()).unwrap();
    let pinned: Vec<&str> = pinned.lines().collect();
    let all = shapes();
    assert_eq!(pinned.len(), all.len(), "one pinned digest per shape");
    let differ: Vec<String> = all
        .iter()
        .zip(&pinned)
        .filter_map(|(shape, want)| {
            let got = digest_line(shape);
            (got != *want).then(|| format!("{want} -> {got}"))
        })
        .collect();
    assert!(
        differ.is_empty(),
        "{} of {} shapes changed their Parquet bytes since v0.32.2 (regenerate only as a deliberate byte change with a CHANGELOG entry: dev/pytools/parquet_golden.py): {differ:#?}",
        differ.len(),
        all.len()
    );
}

#[test]
fn arrow_and_parquet_are_pinned_to_the_versions_that_wrote_the_digests() {
    let manifest =
        std::fs::read_to_string(format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR"))).unwrap();
    for name in ["arrow", "arrow-schema", "parquet"] {
        let line = manifest
            .lines()
            .find(|l| l.starts_with(&format!("{name} = ")))
            .unwrap_or_else(|| panic!("Cargo.toml has no `{name}` line"));
        assert!(
            line.contains("version = \"=59.0.0\""),
            "{name} is not pinned to =59.0.0 ({line}): a newer parquet changes the file bytes or grows the writer's memory with the row count; move the pin only with the digests regenerated as a byte change and the flat_rss gate cell green"
        );
    }
}

#[test]
#[ignore = "writes the digests; run by dev/pytools/parquet_golden.py on the v0.32.2 tree"]
fn parquet_bytes_dump() {
    let lines: Vec<String> = shapes().iter().map(digest_line).collect();
    std::fs::write(digests_path(), lines.join("\n") + "\n").unwrap();
}

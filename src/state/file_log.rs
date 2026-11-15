use crate::error::Result;

use super::StateStore;

/// One row from `file_log` (formerly `file_manifest`; renamed in schema v8).
#[derive(Debug, serde::Serialize)]
#[allow(dead_code)]
pub struct FileRecord {
    pub run_id: String,
    pub export_name: String,
    pub file_name: String,
    pub row_count: i64,
    pub bytes: i64,
    pub format: String,
    pub compression: Option<String>,
    pub created_at: String,
}

/// File log store — reads and writes `file_log`.
pub struct FilePart<'a> {
    pub run_id: &'a str,
    pub export_name: &'a str,
    pub file_name: &'a str,
    pub rows: i64,
    pub bytes: i64,
    pub format: &'a str,
    pub compression: Option<&'a str>,
    /// v25: page high-water key (keyset cursor-atomic checkpoint); `None` for non-keyset parts.
    pub cursor_high: Option<&'a str>,
}

/// One part of one run, as it lands.
pub struct DurablePart<'a> {
    pub run_id: &'a str,
    pub export_name: &'a str,
    pub file_name: &'a str,
    pub rows: i64,
    pub bytes: i64,
    pub format: &'a str,
    pub compression: Option<&'a str>,
    /// The extraction strategy that produced it (`full`, `chunked`, `cdc`, …).
    pub mode: &'a str,
    /// v25: the page's high-water key, for the keyset checkpoint's cursor-atomic resume.
    pub cursor_high: Option<&'a str>,
}

impl StateStore {
    /// ONE part became durable: record the part AND the run's aggregate.
    pub fn record_durable_part(&self, p: DurablePart<'_>) -> Result<()> {
        let DurablePart {
            run_id,
            export_name,
            file_name,
            rows,
            bytes,
            format,
            compression,
            mode,
            cursor_high,
        } = p;
        self.forget_part(run_id, file_name)?;
        self.record_file(FilePart {
            run_id,
            export_name,
            file_name,
            rows,
            bytes,
            format,
            compression,
            cursor_high,
        })?;
        self.project_running_aggregate(run_id, export_name, mode, format)
    }

    /// Drop any prior row for this run's part, so re-recording replaces rather than accumulates.
    fn forget_part(&self, run_id: &str, file_name: &str) -> Result<()> {
        self.execute(
            "DELETE FROM file_log WHERE run_id = ?1 AND file_name = ?2",
            &[run_id.into(), file_name.into()],
        )?;
        Ok(())
    }

    pub fn record_file(&self, p: FilePart<'_>) -> Result<()> {
        let FilePart {
            run_id,
            export_name,
            file_name,
            rows: row_count,
            bytes,
            format,
            compression,
            cursor_high,
        } = p;
        self.execute(
            "INSERT INTO file_log (run_id, export_name, file_name, row_count, bytes, format, compression, created_at, cursor_high) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            &[
                run_id.into(),
                export_name.into(),
                file_name.into(),
                row_count.into(),
                bytes.into(),
                format.into(),
                compression.into(),
                chrono::Utc::now().to_rfc3339().into(),
                cursor_high.into(),
            ],
        )?;
        Ok(())
    }

    pub fn get_files(&self, export_name: Option<&str>, limit: usize) -> Result<Vec<FileRecord>> {
        let cols =
            "run_id, export_name, file_name, row_count, bytes, format, compression, created_at";
        match export_name {
            Some(name) => self.query(
                &format!(
                    "SELECT {cols} FROM file_log WHERE export_name = ?1 ORDER BY id DESC LIMIT ?2"
                ),
                &[name.into(), (limit as i64).into()],
                file_record,
            ),
            None => self.query(
                &format!("SELECT {cols} FROM file_log ORDER BY id DESC LIMIT ?1"),
                &[(limit as i64).into()],
                file_record,
            ),
        }
    }

    /// Every `file_log` row for one `run_id`, in write order.
    pub fn list_files_for_run(&self, run_id: &str) -> Result<Vec<FileRecord>> {
        let cols =
            "run_id, export_name, file_name, row_count, bytes, format, compression, created_at";
        self.query(
            &format!("SELECT {cols} FROM file_log WHERE run_id = ?1 ORDER BY id ASC"),
            &[run_id.into()],
            file_record,
        )
    }

    /// v25: the high-water key of the LAST committed keyset page for `run_id`.
    pub fn last_committed_cursor_high(&self, run_id: &str) -> Result<Option<String>> {
        let rows = self.query(
            "SELECT cursor_high FROM file_log \
             WHERE run_id = ?1 AND cursor_high IS NOT NULL ORDER BY id DESC LIMIT 1",
            &[run_id.into()],
            |r| r.text(0),
        )?;
        Ok(rows.into_iter().next())
    }

    /// Whether `value` is the high-water key of a keyset page `run_id` committed.
    pub(super) fn is_committed_cursor_high(&self, run_id: &str, value: &str) -> Result<bool> {
        let rows = self.query(
            "SELECT 1 FROM file_log WHERE run_id = ?1 AND cursor_high = ?2 LIMIT 1",
            &[run_id.into(), value.into()],
            |r| r.i64(0),
        )?;
        Ok(!rows.is_empty())
    }

    /// A format other than `format` among the parts `run_id` logged.
    pub(super) fn another_format_of_run(
        &self,
        run_id: &str,
        format: &str,
    ) -> Result<Option<String>> {
        if format.is_empty() {
            return Ok(None);
        }
        self.query_opt(
            "SELECT format FROM file_log WHERE run_id = ?1 AND format <> ?2 ORDER BY id LIMIT 1",
            &[run_id.into(), format.into()],
            |r| r.text(0),
        )
    }

    /// Forget the named parts of `run_id`, so a resume that reads their rows again does not declare them too.
    pub fn forget_parts(&self, run_id: &str, file_names: &[String]) -> Result<()> {
        self.transaction(|| {
            file_names
                .iter()
                .try_for_each(|name| self.forget_part(run_id, name))
        })
    }

    /// Forget the parts a keyset page logged before it finished.
    pub fn forget_unfinished_page_parts(&self, run_id: &str) -> Result<usize> {
        self.execute(
            "DELETE FROM file_log WHERE run_id = ?1 AND id > COALESCE( \
             (SELECT MAX(id) FROM file_log WHERE run_id = ?1 AND cursor_high IS NOT NULL), 0)",
            &[run_id.into()],
        )
    }
}

/// The `FileRecord` projection, written once for both backends.
fn file_record(r: &dyn super::row::StateRow) -> FileRecord {
    FileRecord {
        run_id: r.text(0),
        export_name: r.text(1),
        file_name: r.text(2),
        row_count: r.i64(3),
        bytes: r.i64(4),
        format: r.text(5),
        compression: r.opt_text(6),
        created_at: r.text(7),
    }
}

#[cfg(test)]
mod tests {

    /// Recording a durable part writes BOTH records, and the aggregate is a PROJECTION of the parts rather than a second opinion.
    #[test]
    fn recording_a_durable_part_projects_the_run_aggregate_from_the_parts() {
        let s = StateStore::open_in_memory().expect("in-memory state");
        for (i, rows) in [(0, 10i64), (1, 25), (2, 7)] {
            s.record_durable_part(DurablePart {
                run_id: "r1",
                export_name: "orders",
                file_name: &format!("orders_{i}.parquet"),
                rows,
                bytes: 100 * (i + 1) as i64,
                format: "parquet",
                compression: Some("zstd"),
                mode: "chunked",
                cursor_high: None,
            })
            .expect("record");
        }

        let rows = s.get_metrics(Some("orders"), 10).expect("metrics");
        assert_eq!(
            rows.len(),
            1,
            "ONE aggregate per run — a row per part would make every sum over \
             export_metrics count this run three times"
        );
        assert_eq!(rows[0].status, "running", "the run has not finished");
        assert_eq!(
            rows[0].total_rows, 42,
            "the aggregate must equal the SUM of the parts (10+25+7), because it is \
             computed from them"
        );
        assert_eq!(s.list_files_for_run("r1").expect("files").len(), 3);

        s.record_durable_part(DurablePart {
            run_id: "r1",
            export_name: "orders",
            file_name: "orders_0.parquet",
            rows: 10,
            bytes: 100,
            format: "parquet",
            compression: Some("zstd"),
            mode: "chunked",
            cursor_high: None,
        })
        .expect("re-record");
        let again = s.get_metrics(Some("orders"), 10).expect("metrics");
        assert_eq!(
            again[0].total_rows, 42,
            "a re-recorded part must not inflate the aggregate — that is the difference \
             between a projection and an accumulator"
        );
    }

    use super::*;

    /// A page that rotated into parts and crashed before its last one is not committed.
    #[test]
    fn a_resume_forgets_only_the_parts_of_an_unfinished_page() {
        let s = StateStore::open_in_memory().expect("in-memory state");
        let part = |run: &'static str, name: &'static str, high: Option<&'static str>| {
            s.record_durable_part(DurablePart {
                run_id: run,
                export_name: "orders",
                file_name: name,
                rows: 1,
                bytes: 1,
                format: "parquet",
                compression: None,
                mode: "chunked",
                cursor_high: high,
            })
            .expect("record");
        };
        part("r1", "p0a", None);
        part("r1", "p0b", Some("k300"));
        part("r1", "p1a", None);
        part("r1", "p1b", None);
        part("r2", "other", None);
        assert!(s.is_committed_cursor_high("r1", "k300").unwrap());
        assert!(
            !s.is_committed_cursor_high("r2", "k300").unwrap(),
            "another run's page"
        );
        assert!(
            !s.is_committed_cursor_high("r1", "k200").unwrap(),
            "no page of r1 ends there"
        );
        assert_eq!(s.forget_unfinished_page_parts("r1").expect("forget"), 2);
        let kept: Vec<String> = s
            .list_files_for_run("r1")
            .unwrap()
            .into_iter()
            .map(|f| f.file_name)
            .collect();
        assert_eq!(kept, vec!["p0a", "p0b"]);
        assert_eq!(s.list_files_for_run("r2").unwrap().len(), 1);
        part("r3", "q0a", None);
        assert_eq!(s.forget_unfinished_page_parts("r3").expect("forget"), 1);
    }

    fn store() -> StateStore {
        StateStore::open_in_memory().expect("in-memory store")
    }

    #[test]
    fn a_run_names_another_format_among_its_parts_and_forgets_the_parts_it_is_told_to() {
        let s = store();
        let part = |run_id: &str, file_name: &str, format: &str| {
            s.record_file(FilePart {
                run_id,
                export_name: "orders",
                file_name,
                rows: 1,
                bytes: 1,
                format,
                compression: None,
                cursor_high: None,
            })
            .unwrap();
        };
        part("r1", "a.parquet", "parquet");
        part("r1", "b.parquet", "parquet");
        part("r2", "a.parquet", "parquet");
        part("r2", "c.csv", "csv");
        let other = |run: &str, format: &str| s.another_format_of_run(run, format).unwrap();
        assert_eq!(other("r1", "parquet"), None);
        assert_eq!(other("r1", "csv").as_deref(), Some("parquet"));
        assert_eq!(other("r1", ""), None, "a caller that names no format");
        assert_eq!(other("r2", "parquet").as_deref(), Some("csv"));
        assert_eq!(other("r3", "csv"), None, "a run that logged no part");

        s.forget_parts("r1", &["a.parquet".to_string(), "c.csv".to_string()])
            .unwrap();
        let names = |run: &str| -> Vec<String> {
            let files = s.list_files_for_run(run).unwrap();
            files.into_iter().map(|f| f.file_name).collect()
        };
        assert_eq!(names("r1"), ["b.parquet"]);
        assert_eq!(names("r2"), ["a.parquet", "c.csv"], "another run's parts");
    }

    #[test]
    fn record_and_query_files() {
        let s = store();
        s.record_file(FilePart {
            run_id: "run_001",
            export_name: "orders",
            file_name: "orders_20260329.parquet",
            rows: 50000,
            bytes: 4096,
            format: "parquet",
            compression: Some("zstd"),
            cursor_high: None,
        })
        .unwrap();
        s.record_file(FilePart {
            run_id: "run_001",
            export_name: "orders",
            file_name: "orders_20260329_chunk1.parquet",
            rows: 25000,
            bytes: 2048,
            format: "parquet",
            compression: Some("zstd"),
            cursor_high: None,
        })
        .unwrap();
        s.record_file(FilePart {
            run_id: "run_002",
            export_name: "users",
            file_name: "users_20260329.csv",
            rows: 1000,
            bytes: 500,
            format: "csv",
            compression: None,
            cursor_high: None,
        })
        .unwrap();

        let files = s.get_files(Some("orders"), 10).unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].run_id, "run_001");
        assert_eq!(files[0].row_count, 25000);

        let all = s.get_files(None, 10).unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn files_limit_works() {
        let s = store();
        for i in 0..10 {
            s.record_file(FilePart {
                run_id: &format!("r{}", i),
                export_name: "t",
                file_name: &format!("f{}.parquet", i),
                rows: i,
                bytes: i * 100,
                format: "parquet",
                compression: None,
                cursor_high: None,
            })
            .unwrap();
        }
        let files = s.get_files(Some("t"), 3).unwrap();
        assert_eq!(files.len(), 3);
    }
}

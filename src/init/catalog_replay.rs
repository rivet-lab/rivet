//! Offline replay harness for the strategy-decision logic — the **first-init anchor**.

use super::TableInfo;

/// The strategy `rivet init` would scaffold for `info`, as a compact label.
pub(crate) fn scaffold_strategy(info: &TableInfo) -> String {
    match info.suggest_mode() {
        "chunked" => match info.keysettable_pk_column() {
            Some(pk) => format!("keyset({pk})"),
            None => format!("chunked({})", info.best_chunk_column().unwrap_or("id")),
        },
        "incremental" => format!(
            "incremental({})",
            info.chosen_cursor_column().as_deref().unwrap_or("?")
        ),
        other => other.to_string(),
    }
}

/// The FULL scaffold decision `rivet init` writes.
pub(crate) fn scaffold_full(info: &TableInfo, engine: &str) -> String {
    match info.suggest_mode() {
        "chunked" => {
            let size = info.suggest_chunk_size();
            match info.keysettable_pk_column() {
                Some(pk) => format!("keyset({pk}, size={size}, parallel=1, checkpoint)"),
                None => {
                    let col = info.best_chunk_column().unwrap_or("id");
                    let p = super::yaml_scaffold::suggest_parallel(
                        info.row_estimate,
                        info.avg_row_bytes(),
                        engine,
                    )
                    .workers;
                    format!("chunked({col}, size={size}, parallel={p}, checkpoint)")
                }
            }
        }
        "incremental" => format!(
            "incremental({})",
            info.chosen_cursor_column().as_deref().unwrap_or("?")
        ),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init::ColumnInfo;

    fn c(name: &str, ty: &str, pk: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            data_type: ty.into(),
            is_primary_key: pk,
            is_nullable: false,
            numeric_precision: None,
            numeric_scale: None,
            is_indexed: false,
            ..Default::default()
        }
    }

    /// A primary-key column the keyset seam refuses.
    fn refused_pk(name: &str, ty: &str) -> ColumnInfo {
        ColumnInfo {
            not_keyset: true,
            ..c(name, ty, true)
        }
    }

    /// A non-PK column that leads an index.
    fn ix(name: &str, ty: &str) -> ColumnInfo {
        ColumnInfo {
            is_indexed: true,
            ..c(name, ty, false)
        }
    }

    fn tbl(rows: i64, bytes: i64, cols: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            cdc_capture_instance: None,
            cdc_unmineable: None,
            density: None,
            schema: "s".into(),
            table: "t".into(),
            row_estimate: rows,
            total_bytes: Some(bytes),
            columns: cols,
        }
    }

    const ENGINES: [&str; 3] = ["postgres", "mysql", "mssql"];
    fn narrow(rows: i64) -> i64 {
        rows * 64
    }
    fn wide(rows: i64) -> i64 {
        rows * 1500
    }

    /// FULL scaffold decision (strategy + chunk_size + parallel + checkpoint) across EVERY branch, verified on ALL THREE SQL engines.
    #[test]
    fn scaffold_full_decision_across_every_branch_and_engine() {
        let keyset_cases: &[(TableInfo, &str)] = &[
            (
                tbl(
                    150_000,
                    narrow(150_000),
                    vec![c("id", "bigint", true), c("x", "text", false)],
                ),
                "keyset(id, size=100000, parallel=1, checkpoint)",
            ),
            (
                tbl(
                    3_000_000,
                    narrow(3_000_000),
                    vec![c("id", "uuid", true), c("x", "text", false)],
                ),
                "keyset(id, size=250000, parallel=1, checkpoint)",
            ),
            (
                tbl(
                    200_000_000,
                    narrow(200_000_000),
                    vec![c("code", "varchar", true), c("x", "text", false)],
                ),
                "keyset(code, size=2500000, parallel=1, checkpoint)",
            ),
            (
                tbl(
                    2_000_000,
                    narrow(2_000_000),
                    vec![c("fk", "float", true), c("x", "text", false)],
                ),
                "keyset(fk, size=250000, parallel=1, checkpoint)",
            ),
            (
                tbl(
                    600_000,
                    narrow(600_000),
                    vec![c("ts", "timestamp", true), c("x", "text", false)],
                ),
                "keyset(ts, size=100000, parallel=1, checkpoint)",
            ),
            (
                tbl(
                    8_000_000,
                    narrow(8_000_000),
                    vec![c("d", "date", true), c("x", "text", false)],
                ),
                "keyset(d, size=250000, parallel=1, checkpoint)",
            ),
        ];
        for (info, want) in keyset_cases {
            for eng in ENGINES {
                assert_eq!(&scaffold_full(info, eng), want, "keyset on {eng}");
            }
        }

        let range_narrow: &[(TableInfo, &str)] = &[
            (
                tbl(
                    300_000,
                    narrow(300_000),
                    vec![ix("k", "bigint"), c("v", "int", false)],
                ),
                "chunked(k, size=100000, parallel=1, checkpoint)",
            ),
            (
                tbl(
                    2_000_000,
                    narrow(2_000_000),
                    vec![ix("k", "bigint"), c("v", "int", false)],
                ),
                "chunked(k, size=250000, parallel=2, checkpoint)",
            ),
            (
                tbl(
                    10_000_000,
                    narrow(10_000_000),
                    vec![ix("k", "bigint"), c("v", "int", false)],
                ),
                "chunked(k, size=1000000, parallel=4, checkpoint)",
            ),
            (
                tbl(
                    2_000_000,
                    narrow(2_000_000),
                    vec![
                        c("a", "bigint", true),
                        c("b", "bigint", true),
                        c("v", "int", false),
                    ],
                ),
                "chunked(a, size=250000, parallel=2, checkpoint)",
            ),
            (
                tbl(
                    200_000,
                    narrow(200_000),
                    vec![refused_pk("id", "decimal"), ix("seq", "int")],
                ),
                "chunked(seq, size=100000, parallel=1, checkpoint)",
            ),
        ];
        for (info, want) in range_narrow {
            for eng in ENGINES {
                assert_eq!(&scaffold_full(info, eng), want, "range-chunk on {eng}");
            }
        }
        let unindexed = tbl(
            2_000_000,
            narrow(2_000_000),
            vec![c("k", "bigint", false), c("v", "int", false)],
        );
        for eng in ENGINES {
            assert_eq!(
                scaffold_full(&unindexed, eng),
                "full",
                "unindexed key on {eng}"
            );
        }

        let big_wide = tbl(
            10_000_000,
            wide(10_000_000),
            vec![ix("k", "bigint"), c("v", "int", false)],
        );
        assert_eq!(
            scaffold_full(&big_wide, "mysql"),
            "chunked(k, size=1000000, parallel=1, checkpoint)"
        );
        assert_eq!(
            scaffold_full(&big_wide, "postgres"),
            "chunked(k, size=1000000, parallel=4, checkpoint)"
        );
        assert_eq!(
            scaffold_full(&big_wide, "mssql"),
            "chunked(k, size=1000000, parallel=4, checkpoint)"
        );

        let inc = tbl(
            2_000_000,
            narrow(2_000_000),
            vec![
                c("name", "text", false),
                c("updated_at", "timestamp", false),
            ],
        );
        for eng in ENGINES {
            assert_eq!(
                scaffold_full(&inc, eng),
                "incremental(updated_at)",
                "incremental on {eng}"
            );
        }

        let full_cases: &[TableInfo] = &[
            tbl(
                27,
                8_192,
                vec![c("id", "bigint", true), c("x", "text", false)],
            ),
            tbl(
                50_000,
                narrow(50_000),
                vec![c("a", "text", false), c("b", "text", false)],
            ),
            tbl(
                150_000,
                narrow(150_000),
                vec![refused_pk("id", "decimal"), c("name", "text", false)],
            ),
            tbl(
                50_000,
                narrow(50_000),
                vec![c("id", "uuid", true), c("x", "text", false)],
            ),
        ];
        for info in full_cases {
            for eng in ENGINES {
                assert_eq!(scaffold_full(info, eng), "full", "full on {eng}");
            }
        }
    }

    /// The distilled hostile-catalog fixture: representative table SHAPES from the real 153-table field DB (anonymized names, real stats + types + PK shape), plus the expected scaffold strategy.
    const FIXTURE: &str = include_str!("fixtures/hostile_catalog.json");

    #[derive(serde::Deserialize)]
    struct FixtureRow {
        expect: String,
        /// Free-text note (why this shape matters / known-gap marker).
        #[allow(dead_code)]
        note: String,
        table: TableInfo,
    }

    #[test]
    fn scaffold_strategy_matches_every_distilled_field_shape() {
        let rows: Vec<FixtureRow> =
            serde_json::from_str(FIXTURE).expect("hostile_catalog.json must parse");
        assert!(rows.len() >= 6, "fixture must cover several shapes");
        for row in &rows {
            let got = scaffold_strategy(&row.table);
            assert_eq!(
                got, row.expect,
                "table '{}' ({}): expected strategy {}, got {}",
                row.table.table, row.note, row.expect, got
            );
        }
    }

    /// KNOWN GAP (field-observed → ERROR 3024): a table that is small by ROW COUNT but heavy by BYTES (wide TEXT/BLOB columns.
    #[test]
    fn wide_but_short_table_is_currently_full_a_known_row_width_gap() {
        let info: TableInfo = serde_json::from_str(
            r#"{
              "schema": "s", "table": "wide_short_25k",
              "row_estimate": 25000, "total_bytes": 21474836480,
              "columns": [
                {"name":"id","data_type":"bigint","is_primary_key":true,"is_nullable":false,"numeric_precision":null,"numeric_scale":null},
                {"name":"wide_text_a","data_type":"longtext","is_primary_key":false,"is_nullable":true,"numeric_precision":null,"numeric_scale":null},
                {"name":"wide_text_b","data_type":"longtext","is_primary_key":false,"is_nullable":true,"numeric_precision":null,"numeric_scale":null}
              ]
            }"#,
        )
        .unwrap();
        assert_eq!(
            scaffold_strategy(&info),
            "full",
            "documents the row-width gap: this SHOULD be keyset(id) once init weighs total_bytes"
        );
    }
}

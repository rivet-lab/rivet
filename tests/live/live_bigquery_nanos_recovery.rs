//! A SQL Server `datetime2(7)` / `datetimeoffset(7)` lands on BigQuery as INT64 nanoseconds since the epoch, and the expressions docs/type-capability-matrix.yaml `native_recover` names turn it back into a native value that equals the source: graded by BigQuery's own rendering against the text SQL Server renders.

use crate::common::*;

const UNGRADED: &str =
    "the subject is the INT64 the load builds, graded against the source server's own text";

/// `native_recover.bigquery_int64_nanos.<key>` with its `x` bound to `col`.
fn recover(key: &str, col: &str) -> String {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/type-capability-matrix.yaml");
    let doc: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&std::fs::read_to_string(path).expect("the ledger"))
            .expect("the ledger parses");
    let expr = doc["native_recover"]["bigquery_int64_nanos"][key]
        .as_str()
        .unwrap_or_else(|| panic!("native_recover.bigquery_int64_nanos.{key} is missing"));
    let (mut out, mut word) = (String::new(), String::new());
    for ch in expr.chars().chain(std::iter::once(' ')) {
        if ch.is_alphanumeric() || ch == '_' {
            word.push(ch);
            continue;
        }
        out.push_str(if word == "x" { col } else { &word });
        word.clear();
        out.push(ch);
    }
    out.pop();
    out
}

/// `(DATETIME2(7) literal, DATETIMEOFFSET(7) literal)` rows: the epoch, both sides of it, Arrow's nanosecond ends and an offset.
const ROWS: &[(&str, &str)] = &[
    (
        "'2026-06-23 10:00:00.1234567'",
        "'2026-06-23 15:30:00.1234567 +05:30'",
    ),
    (
        "'1970-01-01 00:00:00.0000000'",
        "'1970-01-01 00:00:00.0000000 +00:00'",
    ),
    (
        "'1969-12-31 23:59:59.9999999'",
        "'1969-12-31 23:59:59.9999999 +00:00'",
    ),
    (
        "'1969-12-31 23:59:59.9999990'",
        "'1969-12-31 23:59:59.9999990 +00:00'",
    ),
    (
        "'1900-01-01 00:00:00.0000000'",
        "'1900-01-01 00:00:00.0000001 +00:00'",
    ),
    (
        "'1677-09-21 00:12:43.1452242'",
        "'1677-09-21 00:12:43.1452242 +00:00'",
    ),
    (
        "'2262-04-11 23:47:16.8547758'",
        "'2262-04-11 23:47:16.8547758 +00:00'",
    ),
    (
        "'2000-02-29 12:34:56.1234567'",
        "'2000-02-29 04:34:56.1234567 -08:00'",
    ),
    (
        "'2000-01-01 00:00:00.0000001'",
        "'2000-01-01 14:00:00.0000001 +14:00'",
    ),
];

/// What BigQuery says of column `col` of row `id` through the expressions `suffix` selects (`""` the ledger's plain pair, `"_before_1970"` the floor pair).
struct Recovered {
    ns: i64,
    instant: String,
    datetime: String,
    sub: i64,
    micros: i64,
}

fn recovered(w: &Warehouse, table: &str, col: &str, plain: bool) -> Vec<(i64, Option<Recovered>)> {
    let (instant, datetime, sub) = if plain {
        ("instant", "datetime", "sub_microsecond_ns")
    } else {
        (
            "instant_before_1970",
            "datetime_before_1970",
            "sub_microsecond_ns_before_1970",
        )
    };
    let (instant, datetime, sub) = (
        recover(instant, col),
        recover(datetime, col),
        recover(sub, col),
    );
    let micros = "'%Y-%m-%d %H:%M:%E6S'";
    let rows = w.read_warehouse_rows(&format!(
        "SELECT id, CAST({col} AS STRING) AS ns, FORMAT_TIMESTAMP({micros}, {instant}) AS inst, \
         FORMAT_DATETIME({micros}, {datetime}) AS dt, CAST({sub} AS STRING) AS sub, \
         CAST(UNIX_MICROS({instant}) AS STRING) AS us FROM {} ORDER BY id",
        w.bq_table(table)
    ));
    let int = |v: &serde_json::Value| {
        v.as_str()
            .expect("a string")
            .parse::<i64>()
            .expect("an integer")
    };
    rows.iter()
        .map(|r| {
            let id = int(&r["id"]);
            let row = (!r["ns"].is_null()).then(|| Recovered {
                ns: int(&r["ns"]),
                instant: r["inst"].as_str().expect("instant").to_string(),
                datetime: r["dt"].as_str().expect("datetime").to_string(),
                sub: int(&r["sub"]),
                micros: int(&r["us"]),
            });
            (id, row)
        })
        .collect()
}

/// The source's text for a recovered value: BigQuery's microseconds and the 100 ns digit the sub-microsecond part holds.
fn as_source_text(r: &Recovered) -> String {
    assert_eq!(r.sub % 100, 0, "SQL Server holds 100 ns ticks: {}", r.sub);
    format!("{}{}", r.instant, r.sub / 100)
}

/// A `datetime2(7)` and a `datetimeoffset(7)` column loaded into BigQuery land as INT64, and each documented pair of expressions recovers the source: the floor pair for every row, the plain pair exactly where nanoseconds since the epoch are not negative with a remainder.
#[test]
#[ignore = "live: requires docker compose mssql + BigQuery creds"]
fn bigquery_int64_nanos_recover_the_source_instant_mssql() {
    let e = SqlEngine::Mssql;
    e.alive();
    let Some(mut w) = Warehouse::of(WarehouseKind::BigQuery, "nsrec") else {
        return;
    };
    let (table, _guard) = e.create(
        "nsrec",
        "id BIGINT PRIMARY KEY, t DATETIME2(7) NULL, o DATETIMEOFFSET(7) NULL",
    );
    w.adopt(&table);
    let mut values: Vec<String> = ROWS
        .iter()
        .enumerate()
        .map(|(i, (t, o))| format!("({}, {t}, {o})", i + 1))
        .collect();
    values.push(format!("({}, NULL, NULL)", ROWS.len() + 1));
    e.exec(&format!(
        "INSERT INTO {table} (id, t, o) VALUES {}",
        values.join(", ")
    ));
    let rig = w
        .shape(e.staged(e.rig(&table), "full", &[]), "id")
        .no_oracle(UNGRADED);
    rig.run_ok();
    w.load(&rig);

    for (col, source_expr) in [
        ("t", "CONVERT(NVARCHAR(100), t, 121)"),
        (
            "o",
            "CONVERT(NVARCHAR(100), CAST(SWITCHOFFSET(o, '+00:00') AS DATETIME2(7)), 121)",
        ),
    ] {
        let server = e.id_texts(&table, source_expr);
        let floor = recovered(&w, &table, col, false);
        let plain = recovered(&w, &table, col, true);
        assert_eq!(floor.len(), server.len(), "{col}: every row is read back");
        for (((id, want), (_, f)), (_, p)) in server.iter().zip(&floor).zip(&plain) {
            let (Some(f), Some(p)) = (f, p) else {
                assert!(
                    want.is_none() && f.is_none() && p.is_none(),
                    "{col} id {id}: NULL stays NULL"
                );
                continue;
            };
            let want = want.as_deref().expect("the source holds a value");
            assert_eq!(
                as_source_text(f),
                want,
                "{col} id {id}: the floor pair recovers the source ({} ns)",
                f.ns
            );
            assert_eq!(
                f.datetime, f.instant,
                "{col} id {id}: DATETIME and TIMESTAMP agree"
            );
            assert_eq!(
                i128::from(f.micros) * 1000 + i128::from(f.sub),
                i128::from(f.ns),
                "{col} id {id}: the pair is the INT64"
            );
            assert_eq!(
                i128::from(p.micros) * 1000 + i128::from(p.sub),
                i128::from(p.ns),
                "{col} id {id}: the plain pair is the INT64"
            );
            assert_eq!(
                p.datetime, p.instant,
                "{col} id {id}: plain DATETIME and TIMESTAMP agree"
            );
            if p.ns >= 0 || p.sub == 0 {
                assert_eq!(
                    as_source_text(p),
                    want,
                    "{col} id {id}: the plain pair recovers the source ({} ns)",
                    p.ns
                );
            } else {
                assert!(p.sub < 0, "{col} id {id}: a negative remainder: {}", p.sub);
                assert_eq!(
                    p.micros,
                    f.micros + 1,
                    "{col} id {id}: DIV rounds toward zero, one microsecond above the floor"
                );
            }
        }
    }
}

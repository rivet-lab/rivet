//! `rivet check` refuses what the baseline leg of a `mode: cdc` export refuses, with the run's code and exit class.
//!
//! The first run of an export with `initial: snapshot` writes its baseline through a synthesized `mode: full` leg
//! that the stream's own admission never sees; `check` and `doctor` admit that leg too while it is pending.

use crate::check_override_wire::check_and_run_refuse_alike_by;
use crate::common::*;

const WIRE: Refused<'static> = Refused::by_code("RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", 1);
const FORMAT: Refused<'static> = Refused::by_code("RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED", 1);

fn declare(rig: Rig, column: &str, declared: &str) -> Rig {
    rig.export_line("columns:")
        .export_line(&format!("  {column}: \"{declared}\""))
}

fn as_csv(rig: Rig, column: &str) -> Rig {
    declare(rig.with_format("csv"), column, "decimal(60,10)")
}

fn pending_baseline_is_refused_by_check_like_the_run(mut scn: CdcScenario, want: Refused<'_>) {
    scn.insert(1);
    scn.settle();
    let name = scn.rig.export_name().to_string();
    let rig = a_refused_baseline_may_leave_its_schema_and_checkpoint(scn.rig);
    check_and_run_refuse_alike_by(rig, &name, want);
}

/// Declares the observed schema and range checkpoint a refused baseline leg leaves behind.
fn a_refused_baseline_may_leave_its_schema_and_checkpoint(rig: Rig) -> Rig {
    rig.a_failed_run_may_leave(
        &[Leftover::ObservedSchema, Leftover::ChunkCheckpoint],
        "the refused baseline leg stored the schema it observed and its range checkpoint before the converter refused",
    )
}

fn both_check_spellings_accept(rig: &Rig, name: &str) {
    for args in [
        &["check", "--export", name][..],
        &["check", "--export", name, "--type-report"][..],
    ] {
        let o = rig.cli(args);
        assert_eq!(
            o.status.code(),
            Some(0),
            "{args:?}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
    }
}

fn a_done_baseline_is_not_admitted_again(mut scn: CdcScenario, column: &str) {
    scn.insert(1);
    scn.settle();
    scn.rig.run_ok();
    let name = scn.rig.export_name().to_string();
    scn.rig
        .amend_export_lines(&["columns:", &format!("  {column}: \"string\"")]);
    both_check_spellings_accept(&scn.rig, &name);
}

fn a_pending_baseline_over_columns_the_run_writes_is_accepted(mut scn: CdcScenario) {
    scn.insert(1);
    scn.settle();
    let name = scn.rig.export_name().to_string();
    both_check_spellings_accept(&scn.rig, &name);
    scn.rig.run_ok();
}

fn a_stream_only_export_is_not_admitted_as_a_batch(mut scn: CdcScenario) {
    scn.insert(1);
    scn.settle();
    let name = scn.rig.export_name().to_string();
    both_check_spellings_accept(&scn.rig, &name);
    scn.rig.run_ok();
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_pending_baseline_wire_mismatch_is_refused_by_check_postgres() {
    pending_baseline_is_refused_by_check_like_the_run(
        CdcScenario::pg_with("cdcw_wire_pg", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| {
            declare(r.cdc_line("initial: snapshot"), "v", "string")
        }),
        WIRE,
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_pending_baseline_csv_decimal256_is_refused_by_check_postgres() {
    pending_baseline_is_refused_by_check_like_the_run(
        CdcScenario::pg_with("cdcw_csv_pg", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| {
            as_csv(r.cdc_line("initial: snapshot"), "v")
        }),
        FORMAT,
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_done_baseline_is_not_admitted_again_postgres() {
    a_done_baseline_is_not_admitted_again(
        CdcScenario::pg_with("cdcw_done_pg", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| {
            r.cdc_line("initial: snapshot")
        }),
        "v",
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_stream_only_export_is_not_admitted_as_a_batch_postgres() {
    a_stream_only_export_is_not_admitted_as_a_batch(CdcScenario::pg_with(
        "cdcw_only_pg",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, _| declare(r, "v", "string"),
    ));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_pending_baseline_wire_mismatch_is_refused_by_check_mysql() {
    pending_baseline_is_refused_by_check_like_the_run(
        CdcScenario::mysql_with(
            "cdcw_wire_my",
            "id BIGINT PRIMARY KEY, v VARCHAR(200)",
            |r, _| declare(r.cdc_line("initial: snapshot"), "v", "binary"),
        ),
        WIRE,
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_pending_baseline_csv_decimal256_is_refused_by_check_mysql() {
    pending_baseline_is_refused_by_check_like_the_run(
        CdcScenario::mysql_with("cdcw_csv_my", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| {
            as_csv(r.cdc_line("initial: snapshot"), "v")
        }),
        FORMAT,
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_done_baseline_is_not_admitted_again_mysql() {
    a_done_baseline_is_not_admitted_again(
        CdcScenario::mysql_with("cdcw_done_my", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| {
            r.cdc_line("initial: snapshot")
        }),
        "v",
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_stream_only_export_is_not_admitted_as_a_batch_mysql() {
    a_stream_only_export_is_not_admitted_as_a_batch(CdcScenario::mysql_with(
        "cdcw_only_my",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, _| declare(r, "v", "string"),
    ));
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_pending_baseline_wire_mismatch_is_refused_by_check_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    pending_baseline_is_refused_by_check_like_the_run(
        CdcScenario::mssql_with("cdcw_wire_ms", "id BIGINT PRIMARY KEY, v BIGINT", |r, t| {
            declare(
                r.repoint(&format!("dbo.{t}")).cdc_line("initial: snapshot"),
                "v",
                "string",
            )
        }),
        WIRE,
    );
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_done_baseline_is_not_admitted_again_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    a_done_baseline_is_not_admitted_again(
        CdcScenario::mssql_with("cdcw_done_ms", "id BIGINT PRIMARY KEY, v BIGINT", |r, t| {
            r.repoint(&format!("dbo.{t}")).cdc_line("initial: snapshot")
        }),
        "v",
    );
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_stream_only_export_is_not_admitted_as_a_batch_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    a_stream_only_export_is_not_admitted_as_a_batch(CdcScenario::mssql_with(
        "cdcw_only_ms",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, t| declare(r.repoint(&format!("dbo.{t}")), "v", "string"),
    ));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle (LogMiner)"]
fn a_pending_baseline_wire_mismatch_is_refused_by_check_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    pending_baseline_is_refused_by_check_like_the_run(
        CdcScenario::oracle_with("cdcw_wire_or", |r, _| {
            declare(r.cdc_line("initial: snapshot"), "V", "decimal(50,2)")
        }),
        WIRE,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle (LogMiner)"]
fn a_done_baseline_is_not_admitted_again_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    a_done_baseline_is_not_admitted_again(
        CdcScenario::oracle_with("cdcw_done_or", |r, _| r.cdc_line("initial: snapshot")),
        "V",
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle (LogMiner)"]
fn a_stream_only_export_is_not_admitted_as_a_batch_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    a_stream_only_export_is_not_admitted_as_a_batch(CdcScenario::oracle_with(
        "cdcw_only_or",
        |r, _| declare(r, "V", "string"),
    ));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_pending_baseline_over_columns_the_run_writes_is_accepted_postgres() {
    a_pending_baseline_over_columns_the_run_writes_is_accepted(CdcScenario::pg_with(
        "cdcw_ok_pg",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, _| r.cdc_line("initial: snapshot"),
    ));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_pending_baseline_over_columns_the_run_writes_is_accepted_mysql() {
    a_pending_baseline_over_columns_the_run_writes_is_accepted(CdcScenario::mysql_with(
        "cdcw_ok_my",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, _| r.cdc_line("initial: snapshot"),
    ));
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_pending_baseline_over_columns_the_run_writes_is_accepted_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    a_pending_baseline_over_columns_the_run_writes_is_accepted(CdcScenario::mssql_with(
        "cdcw_ok_ms",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, t| r.repoint(&format!("dbo.{t}")).cdc_line("initial: snapshot"),
    ));
}

#[test]
#[ignore = "live: requires docker compose mongo (replica set)"]
fn a_pending_baseline_over_columns_the_run_writes_is_accepted_mongo() {
    a_pending_baseline_over_columns_the_run_writes_is_accepted(CdcScenario::mongo_with(
        "cdcw_ok_mo",
        |r, _| r.cdc_line("initial: snapshot"),
    ));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle (LogMiner)"]
fn a_pending_baseline_over_columns_the_run_writes_is_accepted_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    a_pending_baseline_over_columns_the_run_writes_is_accepted(CdcScenario::oracle_with(
        "cdcw_ok_or",
        |r, _| r.cdc_line("initial: snapshot"),
    ));
}

/// A numeric, float or text column under an integer override gets the one answer of the override table on the baseline leg and on a stream-only export: the run and both `check` spellings refuse it at plan time, naming the declared type and a remedy a `mode: cdc` export can take.
fn a_non_integer_under_an_integer_override_is_refused_alike_on_every_leg(
    baseline: CdcScenario,
    stream: CdcScenario,
    column: &str,
    literal: &str,
    qualified: impl Fn(&str) -> String,
) {
    for (mut scn, anchored) in [(baseline, false), (stream, true)] {
        if anchored {
            scn.rig.run_ok();
        }
        let t = qualified(&scn.table);
        scn.sql(&format!("INSERT INTO {t} (id, v) VALUES (1, {literal})"));
        scn.settle();
        if anchored {
            scn.rig
                .amend_export_lines(&["columns:", &format!("  {column}: \"int64\"")]);
        }
        let name = scn.rig.export_name().to_string();
        let rig = a_refused_baseline_may_leave_its_schema_and_checkpoint(scn.rig);
        let said = check_and_run_refuse_alike_by(rig, &name, WIRE);
        assert!(said.contains("int64"), "{said}");
        assert!(
            said.contains("a `mode: cdc` export has no `query:`: remove the override"),
            "{said}"
        );
    }
}

fn numeric_scenarios(
    make: impl Fn(&str, bool) -> CdcScenario,
    column: &str,
    literal: &str,
    qualified: impl Fn(&str) -> String,
) {
    a_non_integer_under_an_integer_override_is_refused_alike_on_every_leg(
        make("snap", true),
        make("strm", false),
        column,
        literal,
        qualified,
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_numeric_under_an_integer_override_is_refused_alike_on_every_leg_postgres() {
    numeric_scenarios(
        |tag, snap| {
            CdcScenario::pg_with(
                &format!("cdcn_{tag}_pg"),
                "id BIGINT PRIMARY KEY, v NUMERIC(10,2)",
                move |r, _| {
                    if snap {
                        declare(r.cdc_line("initial: snapshot"), "v", "int64")
                    } else {
                        r
                    }
                },
            )
        },
        "v",
        "43.00",
        str::to_string,
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_text_under_an_integer_override_is_refused_alike_on_every_leg_postgres() {
    numeric_scenarios(
        |tag, snap| {
            CdcScenario::pg_with(
                &format!("cdct_{tag}_pg"),
                "id BIGINT PRIMARY KEY, v TEXT",
                move |r, _| {
                    if snap {
                        declare(r.cdc_line("initial: snapshot"), "v", "int64")
                    } else {
                        r
                    }
                },
            )
        },
        "v",
        "'2'",
        str::to_string,
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_numeric_under_an_integer_override_is_refused_alike_on_every_leg_mysql() {
    numeric_scenarios(
        |tag, snap| {
            CdcScenario::mysql_with(
                &format!("cdcn_{tag}_my"),
                "id BIGINT PRIMARY KEY, v DECIMAL(10,2)",
                move |r, _| {
                    if snap {
                        declare(r.cdc_line("initial: snapshot"), "v", "int64")
                    } else {
                        r
                    }
                },
            )
        },
        "v",
        "43.00",
        str::to_string,
    );
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_numeric_under_an_integer_override_is_refused_alike_on_every_leg_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    numeric_scenarios(
        |tag, snap| {
            CdcScenario::mssql_with(
                &format!("cdcn_{tag}_ms"),
                "id BIGINT PRIMARY KEY, v NUMERIC(10,2)",
                move |r, t| {
                    let r = r.repoint(&format!("dbo.{t}"));
                    if snap {
                        declare(r.cdc_line("initial: snapshot"), "v", "int64")
                    } else {
                        r
                    }
                },
            )
        },
        "v",
        "43.00",
        |t| format!("dbo.{t}"),
    );
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_text_under_an_integer_override_is_refused_alike_on_every_leg_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    numeric_scenarios(
        |tag, snap| {
            CdcScenario::mssql_with(
                &format!("cdct_{tag}_ms"),
                "id BIGINT PRIMARY KEY, v VARCHAR(20)",
                move |r, t| {
                    let r = r.repoint(&format!("dbo.{t}"));
                    if snap {
                        declare(r.cdc_line("initial: snapshot"), "v", "int64")
                    } else {
                        r
                    }
                },
            )
        },
        "v",
        "'2'",
        |t| format!("dbo.{t}"),
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle (LogMiner)"]
fn a_numeric_under_an_integer_override_is_refused_alike_on_every_leg_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    numeric_scenarios(
        |tag, snap| {
            CdcScenario::oracle_cols_with(
                &format!("cdcn_{tag}_or"),
                "id NUMBER(18) PRIMARY KEY, v NUMBER(10,2)",
                move |r, _| {
                    if snap {
                        declare(r.cdc_line("initial: snapshot"), "V", "int64")
                    } else {
                        r
                    }
                },
            )
        },
        "V",
        "43.00",
        str::to_string,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle (LogMiner)"]
fn a_text_under_an_integer_override_is_refused_alike_on_every_leg_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    numeric_scenarios(
        |tag, snap| {
            CdcScenario::oracle_cols_with(
                &format!("cdct_{tag}_or"),
                "id NUMBER(18) PRIMARY KEY, v VARCHAR2(20)",
                move |r, _| {
                    if snap {
                        declare(r.cdc_line("initial: snapshot"), "V", "int64")
                    } else {
                        r
                    }
                },
            )
        },
        "V",
        "'2'",
        str::to_string,
    );
}

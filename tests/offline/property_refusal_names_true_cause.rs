//! Property P4: every refusal is coded and names the true cause, graded per code and per engine.

use rivet::config::SourceType;
use rivet::error::codes;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

const RIVET_BIN: &str = env!("CARGO_BIN_EXE_rivet");
const MATRIX: &str = "refusal_names_true_cause";

const R_PREFIX: &str = "2026-11-08: the oversized-value message opens with an unregistered RIVET_VALUE_TOO_LARGE prefix; closes when the prefix is dropped from both raise sites";
const R_VERIFY: &str = "2026-11-08: validate prints a second code vocabulary absent from errors.md; closes when it joins codes::ALL or the docs list it";
const R_STATE: &str = "2026-11-08: an unopenable SQLite state path stops uncoded; closes when the open error is raised as STATE_NOT_WRITABLE";
const R_CONNECT: &str = "2026-11-08: an unreachable source stops uncoded; closes when the connect error is raised as SOURCE_CONNECT_FAILED";

const R_CMD_ORDER: &str = "2026-11-10: plan, load, compact, reconcile and repair stop at their own precheck before the config error names the true cause; closes when the config error is raised first";
const R_CMD_ACCEPT: &str = "2026-11-10: validate, metrics and journal exit 0 on a config that run refuses; closes when they refuse with the same code";
const R_REMEDY_REPORT: &str = "2026-11-10: the remedy names `rivet reconcile --report`, which reconcile does not accept; closes when the remedy names repair's --report or reconcile's --output";
const R_REMEDY_FORCE: &str = "2026-11-10: the stale-plan remedy names --force without naming `rivet apply`; closes when the message names the apply command";
const R_FIELD_MONGO: &str = "2026-11-10: mongo accepts batch_size 0, an empty or NUL destination path and a 300-char export name with exit 0; closes when each is refused at config load";

/// Accepted RED cells on main, each with the date and what closes it; a new RED or a stale entry fails.
const ACCEPTED_RED: &[(&str, &str)] = &[
    ("cause/RIVET_SOURCE_CONNECT_FAILED/mongo", R_CONNECT),
    ("cause/RIVET_SOURCE_CONNECT_FAILED/mssql", R_CONNECT),
    ("cause/RIVET_SOURCE_CONNECT_FAILED/mysql", R_CONNECT),
    ("cause/RIVET_SOURCE_CONNECT_FAILED/oracle", R_CONNECT),
    ("cause/RIVET_SOURCE_CONNECT_FAILED/postgres", R_CONNECT),
    ("cause/RIVET_STATE_NOT_WRITABLE/mongo", R_STATE),
    ("cause/RIVET_STATE_NOT_WRITABLE/mssql", R_STATE),
    ("cause/RIVET_STATE_NOT_WRITABLE/mysql", R_STATE),
    ("cause/RIVET_STATE_NOT_WRITABLE/oracle", R_STATE),
    ("cause/RIVET_STATE_NOT_WRITABLE/postgres", R_STATE),
    ("decoy/RIVET_STATE_NOT_WRITABLE/*", R_STATE),
    ("decoy/RIVET_SOURCE_CONNECT_FAILED/*", R_CONNECT),
    (
        "cmd/plan/RIVET_CONFIG_SOURCE_URL_SCHEME_MISMATCH/*",
        R_CMD_ORDER,
    ),
    (
        "cmd/load/RIVET_CONFIG_SOURCE_URL_SCHEME_MISMATCH/*",
        R_CMD_ORDER,
    ),
    (
        "cmd/compact/RIVET_CONFIG_SOURCE_URL_SCHEME_MISMATCH/*",
        R_CMD_ORDER,
    ),
    (
        "cmd/reconcile/RIVET_CONFIG_SOURCE_URL_SCHEME_MISMATCH/*",
        R_CMD_ORDER,
    ),
    (
        "cmd/repair/RIVET_CONFIG_SOURCE_URL_SCHEME_MISMATCH/*",
        R_CMD_ORDER,
    ),
    (
        "cmd/validate/RIVET_CONFIG_SOURCE_URL_SCHEME_MISMATCH/*",
        R_CMD_ACCEPT,
    ),
    (
        "cmd/metrics/RIVET_CONFIG_SOURCE_URL_SCHEME_MISMATCH/*",
        R_CMD_ACCEPT,
    ),
    (
        "cmd/journal/RIVET_CONFIG_SOURCE_URL_SCHEME_MISMATCH/*",
        R_CMD_ACCEPT,
    ),
    (
        "field/RIVET_CONFIG_VALUE_OUT_OF_RANGE/batch_size_0/mongo",
        R_FIELD_MONGO,
    ),
    (
        "field/RIVET_CONFIG_PATH_UNSAFE/destination_path_empty/mongo",
        R_FIELD_MONGO,
    ),
    (
        "field/RIVET_CONFIG_PATH_UNSAFE/destination_path_nul/mongo",
        R_FIELD_MONGO,
    ),
    (
        "field/RIVET_CONFIG_PATH_UNSAFE/export_name_300/mongo",
        R_FIELD_MONGO,
    ),
    ("remedy/RIVET_CLI_REPORT_INVALID", R_REMEDY_REPORT),
    ("remedy/RIVET_PLAN_ARTIFACT_STALE", R_REMEDY_FORCE),
    ("source/RIVET_VERIFY_*", R_VERIFY),
    ("source/RIVET_VALUE_TOO_LARGE", R_PREFIX),
];

/// Whether a ledger key accepts a RED cell: an exact key, or a `prefix*` pattern.
fn accepts(key: &str, cell: &str) -> bool {
    match key.strip_suffix('*') {
        Some(prefix) => cell.starts_with(prefix),
        None => key == cell,
    }
}

/// How a code's cause is injected.
#[derive(Clone, Copy)]
enum Inj {
    Offline(fn(SourceType) -> Probe),
    OfflineSql(fn(SourceType) -> Probe),
    Stand,
    Internal,
}

/// One injected cause: the config, the command, and the bytes the refusal must carry.
#[derive(Clone)]
struct Probe {
    yaml: String,
    argv: Vec<String>,
    env: Vec<(&'static str, String)>,
    files: Vec<(&'static str, String)>,
    witness: Vec<String>,
    absent: Vec<&'static str>,
}

/// What the binary left behind: exit, stdout and stderr bytes.
struct Out {
    exit: i32,
    stdout: String,
    stderr: String,
}

const INJECTORS: &[(&str, Inj)] = &[
    ("RIVET_CONFIG_NO_EXPORTS", Inj::Offline(no_exports)),
    (
        "RIVET_CONFIG_CHUNK_COUNT_INVALID",
        Inj::OfflineSql(chunk_count),
    ),
    ("RIVET_CONFIG_CHUNK_BY_DAYS_INVALID", Inj::Stand),
    ("RIVET_CONFIG_CHUNK_COLUMN_NOT_INTEGER", Inj::Stand),
    (
        "RIVET_CONFIG_DUPLICATE_EXPORT",
        Inj::Offline(duplicate_export),
    ),
    ("RIVET_CONFIG_CDC_RESOURCE_CONFLICT", Inj::Stand),
    ("RIVET_CONFIG_CDC_ROLLOVER_INVALID", Inj::Stand),
    ("RIVET_CONFIG_CDC_CONTINUOUS_UNSUPPORTED", Inj::Stand),
    ("RIVET_CONFIG_CSV_LOAD_UNSUPPORTED", Inj::Stand),
    ("RIVET_CONFIG_LOAD_PARTITION_UNSUPPORTED", Inj::Stand),
    ("RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED", Inj::Stand),
    (
        "RIVET_CONFIG_SOURCE_URL_SCHEME_MISMATCH",
        Inj::Offline(scheme_mismatch),
    ),
    ("RIVET_CONFIG_KEYSET_KEY_UUID_OVERRIDE", Inj::Stand),
    ("RIVET_CONFIG_CURSOR_COLUMN_CASE", Inj::Stand),
    ("RIVET_CONFIG_COLUMN_OVERRIDE_CASE", Inj::Stand),
    ("RIVET_CONFIG_FILE_UNREADABLE", Inj::Offline(file_missing)),
    ("RIVET_CONFIG_YAML_INVALID", Inj::Offline(yaml_typo)),
    (
        "RIVET_CONFIG_SOURCE_CONNECTION_INVALID",
        Inj::Offline(half_source),
    ),
    ("RIVET_CONFIG_SECRET_UNSET", Inj::Offline(secret_unset)),
    ("RIVET_CONFIG_GCP_SECRET_UNREADABLE", Inj::Stand),
    (
        "RIVET_CONFIG_EXPORT_SOURCE_INVALID",
        Inj::Offline(no_selector),
    ),
    (
        "RIVET_CONFIG_MODE_SETTING_INVALID",
        Inj::OfflineSql(mode_setting),
    ),
    (
        "RIVET_CONFIG_DESTINATION_AUTH_INVALID",
        Inj::Offline(dest_auth),
    ),
    (
        "RIVET_CONFIG_DESTINATION_ENDPOINT_UNSAFE",
        Inj::Offline(dest_endpoint),
    ),
    (
        "RIVET_CONFIG_VALUE_OUT_OF_RANGE",
        Inj::Offline(out_of_range),
    ),
    ("RIVET_CONFIG_CDC_SHAPE_INVALID", Inj::Offline(cdc_shape)),
    (
        "RIVET_CONFIG_CSV_COMPRESSION_UNSUPPORTED",
        Inj::Offline(csv_codec),
    ),
    (
        "RIVET_CONFIG_TLS_VERIFICATION_DOWNGRADE",
        Inj::Offline(tls_downgrade),
    ),
    ("RIVET_CONFIG_LOAD_BLOCK_INVALID", Inj::Offline(load_block)),
    ("RIVET_CONFIG_PATH_UNSAFE", Inj::Offline(path_unsafe)),
    ("RIVET_CONFIG_ROW_HASH_INVALID", Inj::Offline(row_hash)),
    (
        "RIVET_CONFIG_COLUMN_OVERRIDE_TABLE_UNKNOWN",
        Inj::Offline(override_table),
    ),
    ("RIVET_TYPE_UNSAFE_MAPPING", Inj::Stand),
    ("RIVET_SOURCE_STATEMENT_TIMEOUT", Inj::Stand),
    ("RIVET_SOURCE_CURSOR_FINER_THAN_MICROSECOND", Inj::Stand),
    ("RIVET_SOURCE_CDC_FOREIGN_CHECKPOINT", Inj::Stand),
    ("RIVET_SOURCE_CDC_CHECKPOINT_SHARED", Inj::Stand),
    ("RIVET_SOURCE_CDC_CHECKPOINT_INVALID", Inj::Stand),
    ("RIVET_SOURCE_CDC_LOG_GAP", Inj::Stand),
    ("RIVET_SOURCE_CDC_TRUNCATED", Inj::Stand),
    ("RIVET_SOURCE_CDC_UNDECODABLE", Inj::Stand),
    ("RIVET_SOURCE_CDC_CELL_UNSUPPORTED", Inj::Stand),
    ("RIVET_SOURCE_CDC_PREREQUISITE", Inj::Stand),
    ("RIVET_SOURCE_CDC_ANCHOR_NOT_RELEASED", Inj::Stand),
    ("RIVET_SOURCE_VALUE_UNREPRESENTABLE", Inj::Stand),
    ("RIVET_SOURCE_PROBE_UNREADABLE", Inj::Stand),
    ("RIVET_SOURCE_COLUMNS_CHANGED_UNDER_READ", Inj::Stand),
    ("RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH", Inj::Stand),
    (
        "RIVET_STATE_URL_SCHEME_UNSUPPORTED",
        Inj::Offline(state_scheme),
    ),
    ("RIVET_STATE_URL_LEAVES_STATE_BEHIND", Inj::Stand),
    ("RIVET_SOURCE_SESSION_TIME_ZONE_NOT_APPLIED", Inj::Stand),
    ("RIVET_CLI_CHECK_REPORTED_ERRORS", Inj::Stand),
    ("RIVET_STATE_URL_TLS_REQUIRED", Inj::Stand),
    ("RIVET_STATE_NOT_WRITABLE", Inj::Offline(state_unwritable)),
    ("RIVET_STATE_CURSOR_NOT_STORED", Inj::Stand),
    ("RIVET_STATE_SCHEMA_NEWER", Inj::Stand),
    ("RIVET_STATE_BACKUP_FAILED", Inj::Stand),
    ("RIVET_STATE_CURSOR_OWNER_MISMATCH", Inj::Stand),
    ("RIVET_STATE_CURSOR_INCOMPARABLE", Inj::Stand),
    ("RIVET_STATE_CURSOR_PAST_MANIFEST", Inj::Stand),
    ("RIVET_STATE_CURSOR_STREAM_MISMATCH", Inj::Stand),
    ("RIVET_STATE_CURSOR_DESTINATION_MISMATCH", Inj::Stand),
    ("RIVET_STATE_CURSOR_SOURCE_RESPELLED", Inj::Stand),
    ("RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH", Inj::Stand),
    ("RIVET_STATE_CDC_TABLE_REJOINED", Inj::Stand),
    ("RIVET_STATE_CHUNK_RUN_OWNER_UNKNOWN", Inj::Stand),
    ("RIVET_STATE_CHUNK_RUN_SUPERSEDED", Inj::Stand),
    (
        "RIVET_STATE_KEYSET_SEQUENTIAL_ANCHOR_UNFINISHED",
        Inj::Stand,
    ),
    ("RIVET_STATE_RUN_IN_PROGRESS", Inj::Stand),
    ("RIVET_STATE_UPGRADE_BESIDE_LIVE_RUN", Inj::Stand),
    ("RIVET_STATE_LOCK_TIMEOUT", Inj::Stand),
    ("RIVET_STATE_CHUNK_CHECKPOINT_GONE", Inj::Stand),
    ("RIVET_STATE_CHUNK_PLAN_CHANGED", Inj::Stand),
    ("RIVET_STATE_CHUNK_WINDOWS_BROKEN", Inj::Stand),
    ("RIVET_STATE_LEASE_KEEPER_UNAVAILABLE", Inj::Stand),
    ("RIVET_STATE_LEASE_LOST", Inj::Stand),
    ("RIVET_PLAN_SOURCE_UNREADABLE", Inj::Stand),
    ("RIVET_SOURCE_COLLECTION_NOT_FOUND", Inj::Stand),
    ("RIVET_SOURCE_TABLE_NOT_FOUND", Inj::Stand),
    ("RIVET_DEST_CONTAINER_NOT_FOUND", Inj::Stand),
    ("RIVET_DEST_RELATIVE_PATH_MOVED", Inj::Stand),
    ("RIVET_DEST_ALREADY_COMPLETE", Inj::Stand),
    ("RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE", Inj::Stand),
    ("RIVET_LOAD_COUNT_MISMATCH", Inj::Stand),
    ("RIVET_LOAD_LEASE_LOST_DURING_WRITE", Inj::Stand),
    ("RIVET_LOAD_ADOPTION_COLUMN_MISMATCH", Inj::Stand),
    ("RIVET_LOAD_TARGET_NOT_RIVETS", Inj::Stand),
    ("RIVET_LOAD_IDENTIFIER_UNSAFE", Inj::Stand),
    ("RIVET_LOAD_KEY_COLUMN_INVALID", Inj::Stand),
    ("RIVET_LOAD_PRIMARY_KEY_MISSING", Inj::Stand),
    ("RIVET_LOAD_RESERVED_COLUMN", Inj::Stand),
    ("RIVET_LOAD_LOOKALIKE_COLUMN", Inj::Stand),
    ("RIVET_LOAD_TARGET_COLLISION", Inj::Stand),
    ("RIVET_LOAD_STAGING_PREFIX_INVALID", Inj::Stand),
    ("RIVET_LOAD_DESTINATION_TYPE_UNSUPPORTED", Inj::Stand),
    ("RIVET_LOAD_PARTITION_BUDGET_EXCEEDED", Inj::Stand),
    ("RIVET_LOAD_FILE_LIMIT_EXCEEDED", Inj::Stand),
    ("RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED", Inj::Stand),
    ("RIVET_LOAD_TLS_CA_INVALID", Inj::Stand),
    ("RIVET_LOAD_EMPTY_RUN_UNSUPPORTED", Inj::Stand),
    ("RIVET_LOAD_RUN_NOT_LOADABLE", Inj::Stand),
    ("RIVET_LOAD_STAGING_OBJECT_UNSAFE", Inj::Stand),
    ("RIVET_LOAD_SOURCES_SHARED", Inj::Stand),
    ("RIVET_LOAD_COLUMN_RESPELLED", Inj::Stand),
    ("RIVET_LOAD_TARGET_OBJECT_CONFLICT", Inj::Stand),
    ("RIVET_LOAD_COMPACT_BASE_MISSING", Inj::Stand),
    ("RIVET_LOAD_CHANGELOG_PARTITION_CHANGED", Inj::Stand),
    ("RIVET_LOAD_BUFFER_NOT_COMPACTED", Inj::Stand),
    ("RIVET_LOAD_REBASELINE_OVER_CHANGES", Inj::Stand),
    ("RIVET_LOAD_CDC_POSITION_UNORDERABLE", Inj::Stand),
    ("RIVET_LOAD_TABLE_BUSY", Inj::Stand),
    ("RIVET_LOAD_MANIFEST_INCONSISTENT", Inj::Stand),
    ("RIVET_LOAD_SOURCE_FILE_MISMATCH", Inj::Stand),
    ("RIVET_LOAD_COPY_COUNT_MISMATCH", Inj::Stand),
    ("RIVET_LOAD_LEDGER_UNREADABLE", Inj::Stand),
    ("RIVET_LOAD_RUN_LANDED_DURING_LOAD", Inj::Stand),
    ("RIVET_LOAD_WAREHOUSE_AUTH_FAILED", Inj::Stand),
    ("RIVET_LOAD_WAREHOUSE_ACCESS_DENIED", Inj::Stand),
    ("RIVET_LOAD_WAREHOUSE_OBJECT_NOT_FOUND", Inj::Stand),
    ("RIVET_LOAD_WAREHOUSE_REJECTED", Inj::Stand),
    ("RIVET_LOAD_WAREHOUSE_UNAVAILABLE", Inj::Stand),
    ("RIVET_LOAD_WAREHOUSE_ANSWER_UNREADABLE", Inj::Internal),
    ("RIVET_INTERNAL_LOAD_INVARIANT", Inj::Internal),
    ("RIVET_VALIDATE_FAILED", Inj::Stand),
    ("RIVET_VALIDATE_UNVERIFIED", Inj::Stand),
    ("RIVET_INTERNAL_VALUE_CONVERTER", Inj::Internal),
    ("RIVET_INTERNAL_SPILL", Inj::Internal),
    ("RIVET_INTERNAL_TYPE_BUILDER", Inj::Internal),
    ("RIVET_LOAD_BLOCK_MISSING", Inj::Stand),
    ("RIVET_LOAD_SPEC_SOURCE_UNRESOLVED", Inj::Stand),
    ("RIVET_CLI_EXPORT_UNKNOWN", Inj::Offline(export_unknown)),
    ("RIVET_CLI_TABLE_UNKNOWN", Inj::Stand),
    ("RIVET_CLI_RUN_ID_EXPORT_MISMATCH", Inj::Stand),
    ("RIVET_CLI_OPTION_UNSUPPORTED", Inj::Stand),
    ("RIVET_CLI_REPORT_INVALID", Inj::Stand),
    ("RIVET_CLI_OUTPUT_UNWRITABLE", Inj::Stand),
    ("RIVET_CLI_INPUT_UNREADABLE", Inj::Stand),
    ("RIVET_CLI_EXECUTABLE_UNRESOLVED", Inj::Stand),
    ("RIVET_CLI_TEST_FAULT_INJECTED", Inj::Stand),
    ("RIVET_PLAN_NOTHING_TO_PLAN", Inj::Stand),
    ("RIVET_PLAN_ARTIFACT_STALE", Inj::Stand),
    ("RIVET_PLAN_ARTIFACT_NO_CREDENTIALS", Inj::Stand),
    ("RIVET_PLAN_PARTITION_TOKEN_UNEXPANDED", Inj::Stand),
    ("RIVET_PLAN_REJECTED", Inj::Stand),
    ("RIVET_PLAN_CHUNK_KEY_SPARSE", Inj::Stand),
    ("RIVET_PLAN_CHUNK_KEY_NULL", Inj::Stand),
    ("RIVET_CONFIG_SETTLE_COLUMN_NOT_TEMPORAL", Inj::Stand),
    ("RIVET_CONFIG_SPLIT_PREFIX_UNSTABLE", Inj::Stand),
    ("RIVET_CONFIG_EXPORT_NAME_RESERVED", Inj::Stand),
    ("RIVET_CONFIG_BATCH_MEMORY_EXCEEDED", Inj::Stand),
    ("RIVET_CONFIG_CDC_CAPTURE_INSTANCE_MISMATCH", Inj::Stand),
    ("RIVET_SOURCE_VALUE_TOO_LARGE", Inj::Stand),
    ("RIVET_SOURCE_KEYSET_KEY_COARSE", Inj::Stand),
    ("RIVET_STATE_CHUNK_RUN_MISSING", Inj::Stand),
    ("RIVET_STATE_CHUNK_RUN_UNFINISHED", Inj::Stand),
    ("RIVET_DEST_MANIFEST_MISSING", Inj::Stand),
    ("RIVET_DEST_MANIFEST_NOT_WRITTEN", Inj::Stand),
    ("RIVET_DEST_OBJECT_NOT_FOUND", Inj::Stand),
    ("RIVET_DEST_UPLOAD_CORRUPTED", Inj::Stand),
    ("RIVET_DEST_PREFIX_UNLISTABLE", Inj::Stand),
    ("RIVET_DEST_COMMITTED_PART_GONE", Inj::Stand),
    ("RIVET_DEST_CONTROL_ARTIFACT_INVALID", Inj::Stand),
    ("RIVET_DEST_PART_ROWS_MISMATCH", Inj::Stand),
    ("RIVET_RUN_UNITS_FAILED", Inj::Stand),
    ("RIVET_RUN_RECONCILE_UNVERIFIED", Inj::Stand),
    ("RIVET_INTERNAL_RUN_INVARIANT", Inj::Internal),
    ("RIVET_CONFIG_SOURCE_URL_INVALID", Inj::Stand),
    ("RIVET_CONFIG_SOURCE_OPTION_UNSUPPORTED", Inj::Stand),
    ("RIVET_CONFIG_SOURCE_FEATURE_MISSING", Inj::Stand),
    ("RIVET_SOURCE_CDC_TABLE_UNRESOLVED", Inj::Stand),
    ("RIVET_SOURCE_CDC_TABLE_UNSUPPORTED", Inj::Stand),
    ("RIVET_SOURCE_COLUMNS_UNREADABLE", Inj::Stand),
    ("RIVET_SOURCE_CDC_GRANTS_CHANGED", Inj::Stand),
    ("RIVET_SOURCE_CDC_TRANSACTION_TOO_LARGE", Inj::Stand),
    ("RIVET_SOURCE_QUERY_COLUMN_UNREFERENCEABLE", Inj::Stand),
    ("RIVET_SOURCE_RUNTIME_UNAVAILABLE", Inj::Stand),
    ("RIVET_SOURCE_CONNECT_FAILED", Inj::Offline(connect_refused)),
    ("RIVET_SOURCE_QUERY_FAILED", Inj::Stand),
    ("RIVET_SOURCE_CDC_SPILL_UNLOCKABLE", Inj::Stand),
    ("RIVET_SOURCE_CDC_SPILL_UNWRITABLE", Inj::Stand),
    ("RIVET_INTERNAL_SOURCE_INVARIANT", Inj::Internal),
    ("RIVET_SOURCE_KEYSET_KEY_UNORDERED", Inj::Stand),
    ("RIVET_CLI_VALUE_INVALID", Inj::Stand),
    ("RIVET_CLI_RUN_ID_UNKNOWN", Inj::Stand),
    ("RIVET_CLI_PREFLIGHT_FAILED", Inj::Stand),
    ("RIVET_CLI_PREFLIGHT_UNVERIFIED", Inj::Stand),
    ("RIVET_CONFIG_TARGET_UNKNOWN", Inj::Stand),
    ("RIVET_CONFIG_TLS_CA_INVALID", Inj::Stand),
    ("RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED", Inj::Stand),
    ("RIVET_CONFIG_META_COLUMN_COLLISION", Inj::Stand),
    ("RIVET_CONFIG_CANON_TYPE_UNSUPPORTED", Inj::Stand),
    ("RIVET_CONFIG_QUALITY_COLUMN_UNKNOWN", Inj::Stand),
    ("RIVET_CONFIG_COLUMN_OVERRIDE_TYPE_INVALID", Inj::Stand),
    ("RIVET_CONFIG_DESTINATION_SETTING_MISSING", Inj::Stand),
    ("RIVET_DEST_CREDENTIALS_INVALID", Inj::Stand),
    ("RIVET_DEST_PREFIX_MODE_MISMATCH", Inj::Stand),
    ("RIVET_DEST_IO_RUNTIME_UNAVAILABLE", Inj::Stand),
    ("RIVET_DEST_TOKEN_ENDPOINT_UNAVAILABLE", Inj::Stand),
    ("RIVET_INIT_CDC_NAME_TAKEN", Inj::Stand),
    ("RIVET_INIT_CONFIG_EXISTS", Inj::Stand),
    ("RIVET_INIT_TABLE_NOT_FOUND", Inj::Stand),
    ("RIVET_INIT_TABLE_UNREADABLE", Inj::Stand),
    ("RIVET_INIT_NO_TABLES", Inj::Stand),
    ("RIVET_INIT_NOTHING_EXPORTABLE", Inj::Stand),
    ("RIVET_LOAD_SPLIT_GENERATIONS_MIXED", Inj::Stand),
    ("RIVET_LOAD_SPEC_MISSING", Inj::Stand),
    ("RIVET_MCP_REQUEST_INVALID", Inj::Stand),
    ("RIVET_PLAN_ARTIFACT_INVALID", Inj::Stand),
    ("RIVET_PLAN_CHUNK_KEY_UNUSABLE", Inj::Stand),
    ("RIVET_STATE_BACKEND_FAILED", Inj::Stand),
    ("RIVET_STATE_FILE_CORRUPT", Inj::Offline(state_corrupt)),
    ("RIVET_STATE_COPY_SOURCE_EMPTY", Inj::Stand),
    ("RIVET_STATE_ACCEPT_PARTITIONED", Inj::Stand),
    ("RIVET_STATE_COPY_TARGET_MISSING", Inj::Stand),
    ("RIVET_STATE_COPY_TARGET_NOT_EMPTY", Inj::Stand),
    ("RIVET_STATE_COPY_COUNT_MISMATCH", Inj::Stand),
    ("RIVET_STATE_MIGRATION_LOCKED", Inj::Stand),
    ("RIVET_INTERNAL_DEST_INVARIANT", Inj::Internal),
    ("RIVET_INTERNAL_STATE_INVARIANT", Inj::Internal),
];

const fn same(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

const fn injected(id: &str) -> bool {
    let mut i = 0;
    while i < INJECTORS.len() {
        if same(INJECTORS[i].0, id) {
            return true;
        }
        i += 1;
    }
    false
}

const fn registered(id: &str) -> bool {
    let mut i = 0;
    while i < codes::ALL.len() {
        if same(codes::ALL[i].id, id) {
            return true;
        }
        i += 1;
    }
    false
}

const fn complete() -> bool {
    let mut i = 0;
    while i < codes::ALL.len() {
        if !injected(codes::ALL[i].id) {
            return false;
        }
        i += 1;
    }
    let mut j = 0;
    while j < INJECTORS.len() {
        if !registered(INJECTORS[j].0) {
            return false;
        }
        j += 1;
    }
    true
}

const _: () = assert!(
    complete(),
    "every registered code needs exactly one injector"
);

/// The engines, one arm each so a new `SourceType` variant fails to compile here.
fn engine_name(e: SourceType) -> &'static str {
    match e {
        SourceType::Postgres => "postgres",
        SourceType::Mysql => "mysql",
        SourceType::Mssql => "mssql",
        SourceType::Oracle => "oracle",
        SourceType::Mongo => "mongo",
    }
}

/// A URL naming a closed local port in the engine's own scheme.
fn engine_url(e: SourceType) -> &'static str {
    match e {
        SourceType::Postgres => "postgresql://u:p@127.0.0.1:1/db",
        SourceType::Mysql => "mysql://u:p@127.0.0.1:1/db",
        SourceType::Mssql => "mssql://u:p@127.0.0.1:1/db",
        SourceType::Oracle => "oracle://u:p@127.0.0.1:1/db",
        SourceType::Mongo => "mongodb://127.0.0.1:1/db",
    }
}

/// A URL in another engine's scheme.
fn foreign_url(e: SourceType) -> &'static str {
    match e {
        SourceType::Postgres => "mysql://u:p@127.0.0.1:1/db",
        SourceType::Mysql => "postgresql://u:p@127.0.0.1:1/db",
        SourceType::Mssql => "postgresql://u:p@127.0.0.1:1/db",
        SourceType::Oracle => "postgresql://u:p@127.0.0.1:1/db",
        SourceType::Mongo => "postgresql://u:p@127.0.0.1:1/db",
    }
}

/// Whether the engine reads through SQL, one arm each.
fn sql_engine(e: SourceType) -> bool {
    match e {
        SourceType::Postgres | SourceType::Mysql | SourceType::Mssql | SourceType::Oracle => true,
        SourceType::Mongo => false,
    }
}

const ENGINES: [SourceType; 5] = [
    SourceType::Postgres,
    SourceType::Mysql,
    SourceType::Mssql,
    SourceType::Oracle,
    SourceType::Mongo,
];

fn source_block(e: SourceType, extra: &str) -> String {
    format!(
        "source:\n  type: {}\n  url: \"{}\"\n{extra}",
        engine_name(e),
        engine_url(e)
    )
}

const LOCAL: &str = "    format: parquet\n    destination:\n      type: local\n      path: ./out\n";
const QUERY: &str = "    query: \"SELECT 1\"\n";

fn export(name: &str, extra: &str) -> String {
    format!("  - name: {name}\n{LOCAL}{extra}")
}

fn cmd(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| s.to_string()).collect()
}

fn on(e: SourceType, source_extra: &str, exports: &str, witness: &[&str]) -> Probe {
    Probe {
        yaml: format!("{}exports:\n{exports}", source_block(e, source_extra)),
        argv: cmd(&["check", "-c", "CFG"]),
        env: vec![],
        files: vec![],
        witness: witness.iter().map(|s| s.to_string()).collect(),
        absent: vec![],
    }
}

fn no_exports(e: SourceType) -> Probe {
    let mut p = on(e, "", "", &["at least one export"]);
    p.yaml = format!("{}exports: []\n", source_block(e, ""));
    p
}

fn duplicate_export(e: SourceType) -> Probe {
    let both = format!("{}{}", export("a", QUERY), export("a", QUERY));
    on(e, "", &both, &["duplicate export name 'a'"])
}

fn chunk_count(e: SourceType) -> Probe {
    let x = format!("{QUERY}    mode: chunked\n    chunk_column: id\n    chunk_count: 0\n");
    on(e, "", &export("a", &x), &["chunk_count must be >= 1"])
}

fn yaml_typo(e: SourceType) -> Probe {
    on(
        e,
        "",
        &export("a", "    qeury: \"SELECT 1\"\n"),
        &["unknown field `qeury`"],
    )
}

fn half_source(e: SourceType) -> Probe {
    let mut p = on(e, "", &export("a", QUERY), &["missing 'host'"]);
    p.yaml = format!(
        "source:\n  type: {}\n  user: u\n  database: d\nexports:\n{}",
        engine_name(e),
        export("a", QUERY)
    );
    p
}

fn secret_unset(e: SourceType) -> Probe {
    on(
        e,
        "",
        &export("a", "    query: \"SELECT ${RIVET_PM_P4_SURELY_UNSET}\"\n"),
        &["RIVET_PM_P4_SURELY_UNSET", "unresolved"],
    )
}

fn no_selector(e: SourceType) -> Probe {
    on(
        e,
        "",
        &export("a", ""),
        &["exactly one of 'query', 'query_file', 'table', or 'tables'"],
    )
}

fn mode_setting(e: SourceType) -> Probe {
    let x = format!("{QUERY}    mode: incremental\n");
    on(
        e,
        "",
        &export("a", &x),
        &["incremental mode requires cursor_column"],
    )
}

fn with_dest(e: SourceType, dest: &str, witness: &[&str]) -> Probe {
    let ex = format!("  - name: a\n    format: parquet\n{QUERY}    destination:\n{dest}");
    on(e, "", &ex, witness)
}

fn dest_auth(e: SourceType) -> Probe {
    with_dest(
        e,
        "      type: s3\n      bucket: b\n      access_key_env: AK\n",
        &["both access_key_env and secret_key_env"],
    )
}

fn dest_endpoint(e: SourceType) -> Probe {
    with_dest(
        e,
        "      type: s3\n      bucket: b\n      endpoint: https://collector.example.com\n",
        &["non-loopback host"],
    )
}

fn out_of_range(e: SourceType) -> Probe {
    let x = format!("{QUERY}    compression_level: 30\n");
    on(
        e,
        "",
        &export("a", &x),
        &["compression_level must be 1..22"],
    )
}

fn cdc_shape(e: SourceType) -> Probe {
    on(
        e,
        "",
        &export(
            "a",
            "    mode: cdc\n    tables: [t, t]\n    cdc:\n      slot: s\n",
        ),
        &["duplicate table 't'"],
    )
}

fn csv_codec(e: SourceType) -> Probe {
    let mut p = on(
        e,
        "",
        &export("a", &format!("{QUERY}    compression: gzip\n")),
        &["CSV output does not support compression: gzip"],
    );
    p.yaml = p.yaml.replace("format: parquet", "format: csv");
    p
}

fn tls_downgrade(e: SourceType) -> Probe {
    on(
        e,
        "  tls:\n    mode: verify-full\n    accept_invalid_certs: true\n",
        &export("a", QUERY),
        &["disables certificate verification"],
    )
}

fn load_block(e: SourceType) -> Probe {
    let x = format!("{QUERY}    load:\n      cleanup_source: true\n");
    on(
        e,
        "",
        &export("a", &x),
        &["has a `load:` override but the config has no top-level `load:`"],
    )
}

fn path_unsafe(e: SourceType) -> Probe {
    on(
        e,
        "",
        &export("../escape", QUERY),
        &["is not filename-safe"],
    )
}

fn row_hash(e: SourceType) -> Probe {
    let x = format!("{QUERY}    meta_columns:\n      row_hash: []\n");
    on(
        e,
        "",
        &export("a", &x),
        &["meta_columns.row_hash is an empty list"],
    )
}

fn override_table(e: SourceType) -> Probe {
    let x = format!("{QUERY}    columns: {{ \"users.v\": \"int8\" }}\n");
    on(e, "", &export("a", &x), &["needs a table-shaped export"])
}

fn scheme_mismatch(e: SourceType) -> Probe {
    let mut p = on(e, "", &export("a", QUERY), &["the url scheme names"]);
    p.yaml = p.yaml.replace(engine_url(e), foreign_url(e));
    p
}

fn file_missing(e: SourceType) -> Probe {
    let mut p = on(e, "", &export("a", QUERY), &["not found"]);
    p.argv = cmd(&["check", "-c", "/nonexistent-pm-p4/rivet.yaml"]);
    p
}

fn export_unknown(e: SourceType) -> Probe {
    let mut p = on(e, "", &export("a", QUERY), &["'zzz'", "Known exports: a"]);
    p.argv = cmd(&["run", "-c", "CFG", "--export", "zzz"]);
    p
}

fn state_scheme(e: SourceType) -> Probe {
    let mut p = on(e, "", &export("a", QUERY), &["ftp://"]);
    p.argv = cmd(&["state", "show", "-c", "CFG"]);
    p.env = vec![("RIVET_STATE_URL", "ftp://x".to_string())];
    p
}

fn connect_refused(e: SourceType) -> Probe {
    let mut p = on(e, "", &export("a", QUERY), &["127.0.0.1:1"]);
    p.argv = cmd(&["run", "-c", "CFG"]);
    p.absent = vec!["authentication", "permission denied", "does not exist"];
    p
}

fn state_corrupt(e: SourceType) -> Probe {
    let mut p = on(e, "", &export("a", QUERY), &["CORRUPT"]);
    p.argv = cmd(&["state", "show", "-c", "CFG"]);
    p.files = vec![(".rivet_state.db", "not a sqlite database ".repeat(60))];
    p
}

fn state_unwritable(e: SourceType) -> Probe {
    let mut p = on(e, "", &export("a", QUERY), &["x.db"]);
    p.argv = cmd(&["state", "show", "-c", "CFG"]);
    p.env = vec![(
        "RIVET_STATE_URL",
        "sqlite:/nonexistent-pm-p4/x.db".to_string(),
    )];
    p.absent = vec!["CORRUPT"];
    p
}

/// A refusal-by-design cause with no registry code: clap's own usage errors.
const UNCODED: &[(&str, &[&str])] = &[
    ("unknown_flag", &["check", "--no-such-flag-pm-p4"]),
    ("unknown_subcommand", &["no-such-subcommand-pm-p4"]),
    ("missing_required_arg", &["check"]),
];

struct Doc {
    kind: String,
    exits: BTreeSet<i32>,
    action: String,
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The registry table of `docs/reference/errors.md`, keyed by code.
fn doc_codes() -> BTreeMap<String, Doc> {
    let text = std::fs::read_to_string(root().join("docs/reference/errors.md")).expect("errors.md");
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        if cells.len() < 5 || !cells[1].starts_with("`RIVET_") {
            continue;
        }
        let exits = cells[3]
            .split(|c: char| !c.is_ascii_digit())
            .filter_map(|n| n.parse().ok())
            .collect();
        out.insert(
            cells[1].trim_matches('`').to_string(),
            Doc {
                kind: cells[2].to_string(),
                exits,
                action: cells[4].to_string(),
            },
        );
    }
    out
}

fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read src").flatten() {
        let p = entry.path();
        if p.is_dir() {
            rs_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// Code-shaped names in `src` outside the registry: a message prefix `"RIVET_X:` or a `RIVET_VERIFY_X` token.
fn literal_codes() -> BTreeSet<String> {
    let mut files = vec![];
    rs_files(&root().join("src"), &mut files);
    let mut out = BTreeSet::new();
    for f in files {
        if f.ends_with("src/error.rs") {
            continue;
        }
        for line in std::fs::read_to_string(&f).expect("read").lines() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            let mut rest = line;
            while let Some(i) = rest.find("RIVET_") {
                let tail = &rest[i..];
                let end = tail
                    .find(|c: char| !(c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))
                    .unwrap_or(tail.len());
                let tok = &tail[..end];
                let quoted_prefix = rest[..i].ends_with('"') && tail[end..].starts_with(':');
                let verify = tok.starts_with("RIVET_VERIFY_") && !tok.ends_with('_');
                if (quoted_prefix || verify) && !codes::ALL.iter().any(|c| c.id == tok) {
                    out.insert(tok.to_string());
                }
                rest = &tail[end.max(1)..];
            }
        }
    }
    out
}

/// Words a text classifier might read as a cause, put in place of an export name.
const DECOYS: &[&str] = &["timeout", "login", "token", "403", "denied"];

type TwinProbe = fn(SourceType) -> Probe;
/// A valid counterpart of each offline cause: the same input with the guarded property made valid.
const TWINS: &[(&str, TwinProbe)] = &[
    ("RIVET_CONFIG_NO_EXPORTS", one_export),
    ("RIVET_CONFIG_CHUNK_COUNT_INVALID", twin_chunk_count),
    ("RIVET_CONFIG_DUPLICATE_EXPORT", one_export),
    ("RIVET_CONFIG_SOURCE_URL_SCHEME_MISMATCH", one_export),
    ("RIVET_CONFIG_FILE_UNREADABLE", one_export),
    ("RIVET_CONFIG_YAML_INVALID", one_export),
    ("RIVET_CONFIG_SOURCE_CONNECTION_INVALID", one_export),
    ("RIVET_CONFIG_SECRET_UNSET", twin_secret_set),
    ("RIVET_CONFIG_EXPORT_SOURCE_INVALID", one_export),
    ("RIVET_CONFIG_MODE_SETTING_INVALID", twin_incremental),
    ("RIVET_CONFIG_DESTINATION_AUTH_INVALID", twin_s3_plain),
    ("RIVET_CONFIG_DESTINATION_ENDPOINT_UNSAFE", twin_s3_loopback),
    ("RIVET_CONFIG_VALUE_OUT_OF_RANGE", twin_level_3),
    ("RIVET_CONFIG_CDC_SHAPE_INVALID", twin_cdc_two_tables),
    ("RIVET_CONFIG_CSV_COMPRESSION_UNSUPPORTED", twin_csv),
    ("RIVET_CONFIG_TLS_VERIFICATION_DOWNGRADE", twin_tls_verified),
    ("RIVET_CONFIG_LOAD_BLOCK_INVALID", one_export),
    ("RIVET_CONFIG_PATH_UNSAFE", twin_safe_name),
    ("RIVET_CONFIG_ROW_HASH_INVALID", twin_row_hash),
    ("RIVET_CONFIG_COLUMN_OVERRIDE_TABLE_UNKNOWN", one_export),
    ("RIVET_STATE_URL_SCHEME_UNSUPPORTED", twin_state_sqlite),
    ("RIVET_STATE_NOT_WRITABLE", twin_state_sqlite),
    ("RIVET_STATE_FILE_CORRUPT", twin_state_absent),
    ("RIVET_CLI_EXPORT_UNKNOWN", twin_run_export_a),
];

type FieldProbe = fn(SourceType) -> Probe;
/// Offline causes with a value other than the one in the injector: `(code, label, probe)`.
const FIELDS: &[(&str, &str, FieldProbe)] = &[
    (
        "RIVET_CONFIG_VALUE_OUT_OF_RANGE",
        "batch_size_0",
        field_batch_zero,
    ),
    (
        "RIVET_CONFIG_PATH_UNSAFE",
        "destination_path_empty",
        field_path_empty,
    ),
    (
        "RIVET_CONFIG_PATH_UNSAFE",
        "destination_path_nul",
        field_path_nul,
    ),
    (
        "RIVET_CONFIG_PATH_UNSAFE",
        "export_name_300",
        field_name_300,
    ),
];

fn one_export(e: SourceType) -> Probe {
    on(e, "", &export("a", QUERY), &[])
}

fn twin_chunk_count(e: SourceType) -> Probe {
    let x = format!("{QUERY}    mode: chunked\n    chunk_column: id\n    chunk_count: 2\n");
    on(e, "", &export("a", &x), &[])
}

fn twin_secret_set(e: SourceType) -> Probe {
    let mut p = on(
        e,
        "",
        &export("a", "    query: \"SELECT ${RIVET_PM_P4_SURELY_UNSET}\"\n"),
        &[],
    );
    p.env = vec![("RIVET_PM_P4_SURELY_UNSET", "1".to_string())];
    p
}

fn twin_incremental(e: SourceType) -> Probe {
    let x = format!("{QUERY}    mode: incremental\n    cursor_column: id\n");
    on(e, "", &export("a", &x), &[])
}

fn twin_s3_plain(e: SourceType) -> Probe {
    with_dest(e, "      type: s3\n      bucket: b\n", &[])
}

fn twin_s3_loopback(e: SourceType) -> Probe {
    with_dest(
        e,
        "      type: s3\n      bucket: b\n      endpoint: http://127.0.0.1:9\n",
        &[],
    )
}

fn twin_level_3(e: SourceType) -> Probe {
    let x = format!("{QUERY}    compression_level: 3\n");
    on(e, "", &export("a", &x), &[])
}

/// Two tables in one export, or one table per export on SQL Server, where `tables:` is refused.
fn twin_cdc_two_tables(e: SourceType) -> Probe {
    if matches!(e, SourceType::Mssql) {
        return on(
            e,
            "",
            &export(
                "a",
                "    mode: cdc\n    table: t\n    cdc:\n      slot: s\n",
            ),
            &[],
        );
    }
    on(
        e,
        "",
        &export(
            "a",
            "    mode: cdc\n    tables: [t, u]\n    cdc:\n      slot: s\n",
        ),
        &[],
    )
}

fn twin_csv(e: SourceType) -> Probe {
    let mut p = one_export(e);
    p.yaml = p.yaml.replace("format: parquet", "format: csv");
    p
}

fn twin_tls_verified(e: SourceType) -> Probe {
    on(
        e,
        "  tls:\n    mode: verify-full\n",
        &export("a", QUERY),
        &[],
    )
}

fn twin_safe_name(e: SourceType) -> Probe {
    on(e, "", &export("escape", QUERY), &[])
}

fn twin_row_hash(e: SourceType) -> Probe {
    let x = format!("{QUERY}    meta_columns:\n      row_hash: [id]\n");
    on(e, "", &export("a", &x), &[])
}

fn twin_state_sqlite(e: SourceType) -> Probe {
    let mut p = one_export(e);
    p.argv = cmd(&["state", "show", "-c", "CFG"]);
    p.env = vec![("RIVET_STATE_URL", "sqlite:x.db".to_string())];
    p
}

fn twin_state_absent(e: SourceType) -> Probe {
    let mut p = one_export(e);
    p.argv = cmd(&["state", "show", "-c", "CFG"]);
    p
}

fn twin_run_export_a(e: SourceType) -> Probe {
    let mut p = one_export(e);
    p.argv = cmd(&["run", "-c", "CFG", "--export", "a"]);
    p
}

fn field_batch_zero(e: SourceType) -> Probe {
    let x = format!("{QUERY}    tuning:\n      batch_size: 0\n");
    on(e, "", &export("a", &x), &["batch_size"])
}

fn field_path_empty(e: SourceType) -> Probe {
    let mut p = one_export(e);
    p.yaml = p.yaml.replace("path: ./out", "path: \"\"");
    p
}

fn field_path_nul(e: SourceType) -> Probe {
    let mut p = one_export(e);
    p.yaml = p.yaml.replace("path: ./out", "path: \"a\\0b\"");
    p
}

fn field_name_300(e: SourceType) -> Probe {
    on(e, "", &export(&"a".repeat(300), QUERY), &[])
}

/// Every decoy and command variant of one offline probe, each graded like the base cell.
fn offline_axes(
    id: &str,
    doc: &Doc,
    e: SourceType,
    p: &Probe,
    commands: &[String],
    out: &mut Vec<(String, Verdict)>,
) {
    let eng = engine_name(e);
    let twin = match TWINS.iter().find(|(t, _)| *t == id) {
        Some((_, f)) => {
            let t = f(e);
            verdict(grade_twin(id, p, &execute(&t)))
        }
        None => Verdict::NotGraded(
            "no offline valid twin: a valid source needs a live engine".to_string(),
        ),
    };
    out.push((format!("twin/{id}/{eng}"), twin));
    if p.yaml.contains("  - name: a\n") {
        for word in DECOYS {
            let q = renamed(p, word);
            let v = verdict(grade(id, doc, &q, &execute(&q)));
            out.push((format!("decoy/{id}/{word}/{eng}"), v));
        }
    }
    if p.argv.first().map(String::as_str) == Some("check") && p.argv.iter().any(|a| a == "CFG") {
        let mut q = p.clone();
        q.argv.insert(0, "--json-errors".to_string());
        out.push((
            format!("json/{id}/{eng}"),
            verdict(grade_json(id, &execute(&q))),
        ));
        for sub in commands.iter().filter(|s| s.as_str() != "check") {
            let mut q = p.clone();
            let mut args = vec![sub.as_str(), "-c", "CFG"];
            if help(&[sub.as_str()]).contains("--export <") {
                args.extend(["--export", "a"]);
            }
            q.argv = cmd(&args);
            let o = execute(&q);
            let v = if sub == "doctor" {
                grade_doctor(id, doc, &q, &o)
            } else {
                grade(id, doc, &q, &o)
            };
            out.push((format!("cmd/{sub}/{id}/{eng}"), verdict(v)));
        }
    }
}

/// Grades `doctor` by the cause on its `[FAIL]` lines, since its summary line carries a generic code.
fn grade_doctor(id: &str, doc: &Doc, p: &Probe, o: &Out) -> Result<String, String> {
    let fails: Vec<&str> = o
        .stdout
        .lines()
        .filter(|l| l.starts_with("[FAIL]"))
        .collect();
    if fails.is_empty() {
        return grade(id, doc, p, o);
    }
    let synthetic = Out {
        exit: o.exit,
        stdout: String::new(),
        stderr: format!("Error: [{id}] {}", fails.join(" ")),
    };
    grade(id, doc, p, &synthetic)
}

fn verdict(r: Result<String, String>) -> Verdict {
    match r {
        Ok(ev) => Verdict::Pass(ev),
        Err(ev) => Verdict::Red(ev),
    }
}

/// Runs the binary in a scratch directory with the probe's config, files and env.
fn execute(p: &Probe) -> Out {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = dir.path().join("c.yaml");
    std::fs::write(&cfg, &p.yaml).expect("write config");
    for (name, body) in &p.files {
        std::fs::write(dir.path().join(name), body).expect("write file");
    }
    let args: Vec<String> = p
        .argv
        .iter()
        .map(|a| {
            if a == "CFG" {
                cfg.display().to_string()
            } else {
                a.clone()
            }
        })
        .collect();
    let mut c = Command::new(RIVET_BIN);
    c.args(&args)
        .current_dir(dir.path())
        .env_remove("RIVET_STATE_URL")
        .env_remove("RIVET_TEST_STATE_URL");
    for (k, v) in &p.env {
        c.env(k, v);
    }
    let o = c.output().expect("spawn rivet");
    Out {
        exit: o.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&o.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
    }
}

/// The text from the final `Error: ` line on, which is where rivet prints the refusal.
fn error_section(stderr: &str) -> Option<&str> {
    stderr.rfind("Error: ").map(|i| &stderr[i..])
}

/// The `[RIVET_...]` code the error section opens with.
fn printed_code(section: &str) -> Option<&str> {
    let rest = section.strip_prefix("Error: [")?;
    let id = &rest[..rest.find(']')?];
    id.starts_with("RIVET_").then_some(id)
}

fn brief(s: &str) -> String {
    let flat: String = s.chars().map(|c| if c == '\n' { ' ' } else { c }).collect();
    flat.chars().take(110).collect()
}

/// Grades one run against the injected code: `Ok(evidence)` is PASS, `Err(evidence)` is RED.
fn grade(id: &str, doc: &Doc, p: &Probe, o: &Out) -> Result<String, String> {
    let Some(section) = error_section(&o.stderr) else {
        return Err(format!(
            "uncoded: no `Error:` line, exit={} stderr=\"{}\"",
            o.exit,
            brief(o.stderr.trim())
        ));
    };
    let Some(printed) = printed_code(section) else {
        return Err(format!("uncoded: exit={} \"{}\"", o.exit, brief(section)));
    };
    if printed != id {
        return Err(format!(
            "printed {printed}, injected {id}: \"{}\"",
            brief(section)
        ));
    }
    if !doc.exits.contains(&o.exit) {
        return Err(format!(
            "exit {} outside the documented {:?} for {id}",
            o.exit, doc.exits
        ));
    }
    if let Some(w) = p.witness.iter().find(|w| !section.contains(w.as_str())) {
        return Err(format!(
            "message does not name the cause: missing `{w}` in \"{}\"",
            brief(section)
        ));
    }
    if let Some(a) = p
        .absent
        .iter()
        .find(|a| section.to_lowercase().contains(**a))
    {
        return Err(format!(
            "message names another cause: `{a}` in \"{}\"",
            brief(section)
        ));
    }
    if let Some(why) = message_shape(section) {
        return Err(format!("{why} in \"{}\"", brief(section)));
    }
    Ok(format!("exit={} \"{}\"", o.exit, brief(section)))
}

/// The first message-shape fault in a refusal: a Rust Debug repr or an equal `N exceeds N` pair.
fn message_shape(s: &str) -> Option<String> {
    let debug =
        regex::Regex::new(r"\b(Some|None|Ok|Err)\(|\b[A-Z][A-Za-z]+ \{ [a-z_]+: ").expect("re");
    if let Some(m) = debug.find(s) {
        return Some(format!("rust Debug repr `{}`", m.as_str()));
    }
    let pair =
        regex::Regex::new(r"(\d+(?:\.\d+)?) ?(?:[A-Za-z]+ )?exceeds (\d+(?:\.\d+)?)").expect("re");
    pair.captures_iter(s)
        .find(|c| c[1] == c[2])
        .map(|c| format!("equal pair `{}`", &c[0]))
}

/// Grades a cause that has no code: it must fail without any registry code.
fn grade_uncoded(o: &Out) -> Result<String, String> {
    let all = format!("{}{}", o.stdout, o.stderr);
    if all.contains("[RIVET_") {
        return Err(format!(
            "coded refusal for an uncoded cause: \"{}\"",
            brief(&o.stderr)
        ));
    }
    if o.exit == 0 {
        return Err("an uncoded cause exited 0".to_string());
    }
    Ok(format!("exit={} \"{}\"", o.exit, brief(o.stderr.trim())))
}

/// Grades a valid counterpart: no registry code of the cause, and none of its witness words.
fn grade_twin(id: &str, base: &Probe, o: &Out) -> Result<String, String> {
    let all = format!("{}{}", o.stdout, o.stderr);
    if all.contains(&format!("[{id}]")) {
        return Err(format!(
            "a valid input is refused by {id}: \"{}\"",
            brief(o.stderr.trim())
        ));
    }
    if let Some(w) = base.witness.iter().find(|w| o.stderr.contains(w.as_str())) {
        return Err(format!(
            "a valid input prints the cause's words `{w}`: \"{}\"",
            brief(o.stderr.trim())
        ));
    }
    Ok(format!("exit={} \"{}\"", o.exit, brief(o.stderr.trim())))
}

/// The probe with its export renamed to a decoy word that a text classifier might read as a cause.
fn renamed(p: &Probe, word: &str) -> Probe {
    let mut q = p.clone();
    q.yaml = q
        .yaml
        .replace("  - name: a\n", &format!("  - name: {word}\n"));
    q.witness = q
        .witness
        .iter()
        .map(|w| {
            w.replace("'a'", &format!("'{word}'"))
                .replace("exports: a", &format!("exports: {word}"))
        })
        .collect();
    q
}

/// Grades a `--json-errors` run: some JSON object on either stream carries the injected code.
fn grade_json(id: &str, o: &Out) -> Result<String, String> {
    let codes: Vec<String> = format!("{}\n{}", o.stdout, o.stderr)
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .filter_map(|v| v.get("code").and_then(|c| c.as_str()).map(str::to_string))
        .collect();
    match codes.last() {
        Some(c) if c == id => Ok(format!("exit={} code={c}", o.exit)),
        Some(c) => Err(format!("json code {c}, injected {id}")),
        None => Err(format!(
            "no json error object: exit={} \"{}\"",
            o.exit,
            brief(o.stderr.trim())
        )),
    }
}

/// The subcommands clap lists under `rivet --help`, and the ones that take `--config`.
fn subcommands() -> (Vec<String>, Vec<String>) {
    let top = help(&[]);
    let mut subs = vec![];
    let mut in_block = false;
    for line in top.lines() {
        if line.starts_with("Commands:") {
            in_block = true;
            continue;
        }
        if in_block {
            if line.trim().is_empty() {
                break;
            }
            if let Some(name) = line.split_whitespace().next()
                && !name.starts_with('-')
                && name != "help"
            {
                subs.push(name.to_string());
            }
        }
    }
    let with_config = subs
        .iter()
        .filter(|s| help(&[s.as_str()]).contains("--config"))
        .cloned()
        .collect();
    (subs, with_config)
}

/// The `--help` text of `rivet <args>`, from the binary itself.
fn help(args: &[&str]) -> String {
    let o = Command::new(RIVET_BIN)
        .args(args)
        .arg("--help")
        .output()
        .expect("spawn rivet");
    format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

/// Every `--flag` a remedy names, with the command words after `rivet` it belongs to.
fn remedy_flags(action: &str) -> Vec<(Vec<String>, String)> {
    let mut out = vec![];
    for seg in action.split("rivet ").skip(1) {
        let words: Vec<String> = seg
            .split_whitespace()
            .map(|w| {
                w.trim_matches(|c: char| "`.,;:()'\"".contains(c))
                    .to_string()
            })
            .collect();
        let first = words.first().cloned().unwrap_or_default();
        let second = words
            .get(1)
            .filter(|w| {
                !w.starts_with('-') && w.chars().all(|c| c.is_ascii_lowercase() || c == '-')
            })
            .cloned();
        for w in words.iter().skip(1) {
            if w.starts_with("--")
                && w.len() > 2
                && w[2..].chars().all(|c| c.is_ascii_lowercase() || c == '-')
            {
                let mut cmd = vec![first.clone()];
                cmd.extend(second.clone());
                out.push((cmd, w.clone()));
            }
        }
    }
    out
}

/// Grades a remedy statically: it exists, and every flag it names is in the help of the command it names.
fn grade_remedy(doc: &Doc, subs: &[String]) -> Result<String, String> {
    if doc.action.trim().is_empty() {
        return Err("no remedy text".to_string());
    }
    let flags = remedy_flags(&doc.action);
    for (cmd, flag) in &flags {
        let first = &cmd[0];
        let helps: Vec<String> = if subs.contains(first) {
            let mut v = vec![help(&[first.as_str()])];
            if let Some(second) = cmd.get(1) {
                let h = help(&[first.as_str(), second.as_str()]);
                if !h.contains("error:") {
                    v.push(h);
                }
            }
            v
        } else if first.starts_with('-') || first.is_empty() {
            vec![help(&[])]
        } else {
            continue;
        };
        if !helps.iter().any(|h| h.contains(flag.as_str())) {
            return Err(format!(
                "remedy names `{flag}`, absent from `rivet {} --help`",
                cmd.join(" ")
            ));
        }
    }
    Ok(format!("{} flag(s) named, all parse", flags.len()))
}

enum Verdict {
    Pass(String),
    Red(String),
    NotGraded(String),
}

fn sabotage_rows() -> BTreeSet<String> {
    let text = std::fs::read_to_string(root().join("docs/sabotage-matrix.yaml")).expect("sabotage");
    text.lines()
        .filter_map(|l| l.trim().strip_prefix("- id: "))
        .map(str::to_string)
        .collect()
}

fn line(key: &str, v: &Verdict) -> String {
    match v {
        Verdict::Pass(e) => format!("{MATRIX} {key} PASS {e}"),
        Verdict::Red(e) => format!("{MATRIX} {key} RED {e}"),
        Verdict::NotGraded(e) => format!("{MATRIX} {key} NOT_GRADED {e}"),
    }
}

/// Every cell of the matrix, as `(key, verdict)`.
fn cells() -> Vec<(String, Verdict)> {
    let docs = doc_codes();
    let rows = sabotage_rows();
    let mut out: Vec<(String, Verdict)> = vec![];
    let (subs, commands) = subcommands();
    let registry: BTreeSet<&str> = codes::ALL.iter().map(|c| c.id).collect();
    let mut every: BTreeSet<String> = docs.keys().cloned().collect();
    every.extend(registry.iter().map(|s| s.to_string()));
    every.extend(literal_codes());
    for id in &every {
        let (in_doc, in_reg) = (docs.contains_key(id), registry.contains(id.as_str()));
        let v = match (in_doc, in_reg) {
            (true, true) => Verdict::Pass("in errors.md and codes::ALL".to_string()),
            (true, false) => Verdict::Red("in errors.md, no coded constant".to_string()),
            (false, true) => Verdict::Red("coded constant, absent from errors.md".to_string()),
            (false, false) => {
                Verdict::Red("named in src, in neither errors.md nor codes::ALL".to_string())
            }
        };
        out.push((format!("source/{id}"), v));
    }
    for (id, inj) in INJECTORS {
        let Some(doc) = docs.get(*id) else { continue };
        match inj {
            Inj::Offline(f) | Inj::OfflineSql(f) => {
                for e in ENGINES {
                    if matches!(inj, Inj::OfflineSql(_)) && !sql_engine(e) {
                        let ev = format!("{} has no SQL read path for this cause", engine_name(e));
                        out.push((
                            format!("cause/{id}/{}", engine_name(e)),
                            Verdict::NotGraded(ev),
                        ));
                        continue;
                    }
                    let p = f(e);
                    let o = execute(&p);
                    let v = verdict(grade(id, doc, &p, &o));
                    out.push((format!("cause/{id}/{}", engine_name(e)), v));
                    offline_axes(id, doc, e, &p, &commands, &mut out);
                }
            }
            Inj::Stand => {
                let row = format!("refusal_{}", id.trim_start_matches("RIVET_").to_lowercase());
                let ev = if rows.contains(&row) {
                    format!("needs the live stand; sabotage row {row} injects it")
                } else {
                    format!(
                        "needs the live stand; no sabotage row {row}, kind {}",
                        doc.kind
                    )
                };
                out.push((format!("cause/{id}/-"), Verdict::NotGraded(ev)));
            }
            Inj::Internal => {
                let ev = format!(
                    "kind {}: an invariant, no operator cause to inject",
                    doc.kind
                );
                out.push((format!("cause/{id}/-"), Verdict::NotGraded(ev)));
            }
        }
    }
    for (id, label, f) in FIELDS {
        let Some(doc) = docs.get(*id) else { continue };
        for e in ENGINES {
            let p = f(e);
            let v = match verdict(grade(id, doc, &p, &execute(&p))) {
                Verdict::Red(ev) if ev.contains("nothing is listening") => Verdict::NotGraded(
                    "connect runs before this check; the offline stand refuses first, graded live"
                        .to_string(),
                ),
                v => v,
            };
            out.push((format!("field/{id}/{label}/{}", engine_name(e)), v));
        }
    }
    for (id, doc) in &docs {
        out.push((format!("remedy/{id}"), verdict(grade_remedy(doc, &subs))));
    }
    for (name, argv) in UNCODED {
        let p = Probe {
            yaml: String::new(),
            argv: cmd(argv),
            env: vec![],
            files: vec![],
            witness: vec![],
            absent: vec![],
        };
        let v = match grade_uncoded(&execute(&p)) {
            Ok(ev) => Verdict::Pass(ev),
            Err(ev) => Verdict::Red(ev),
        };
        out.push((format!("uncoded/{name}"), v));
    }
    out
}

#[test]
fn every_refusal_is_coded_and_names_the_true_cause() {
    let cells = cells();
    let mut text = String::new();
    let (mut pass, mut red, mut ng) = (0, 0, 0);
    let mut reds = BTreeSet::new();
    for (key, v) in &cells {
        match v {
            Verdict::Pass(_) => pass += 1,
            Verdict::Red(_) => {
                red += 1;
                reds.insert(key.clone());
            }
            Verdict::NotGraded(_) => ng += 1,
        }
        text.push_str(&line(key, v));
        text.push('\n');
    }
    text.push_str(&format!(
        "{MATRIX} SUMMARY PASS={pass} RED={red} NOT_GRADED={ng}\n"
    ));
    let sink = Path::new(env!("CARGO_TARGET_TMPDIR")).join("refusal_names_true_cause_verdicts.txt");
    std::fs::write(&sink, &text).expect("write verdicts");
    print!("{text}");
    let fresh: Vec<&String> = reds
        .iter()
        .filter(|r| !ACCEPTED_RED.iter().any(|(k, _)| accepts(k, r)))
        .collect();
    let stale: Vec<&str> = ACCEPTED_RED
        .iter()
        .map(|(k, _)| *k)
        .filter(|k| !reds.iter().any(|r| accepts(k, r)))
        .collect();
    assert_eq!(
        fresh,
        Vec::<&String>::new(),
        "new RED cells not in ACCEPTED_RED"
    );
    assert_eq!(
        stale,
        Vec::<&str>::new(),
        "ACCEPTED_RED entries no longer RED, remove them"
    );
}

#[test]
fn the_engine_axis_is_every_source_type() {
    let derived = super::chunking_matrix_guard::source_engine_variants();
    let used: BTreeSet<String> = ENGINES
        .iter()
        .map(|e| engine_name(*e).to_string())
        .collect();
    assert_eq!(used, derived.into_iter().collect::<BTreeSet<_>>());
}

#[test]
fn every_accepted_red_carries_a_dated_reason() {
    for (key, why) in ACCEPTED_RED {
        assert!(
            why.starts_with("2026-"),
            "{key}: the reason starts with its date"
        );
        assert!(why.len() > 16, "{key}: the reason says what closes it");
    }
}

fn probe_with(witness: &[&str], absent: &[&'static str]) -> Probe {
    Probe {
        yaml: String::new(),
        argv: vec![],
        env: vec![],
        files: vec![],
        witness: witness.iter().map(|s| s.to_string()).collect(),
        absent: absent.to_vec(),
    }
}

fn run_out(exit: i32, stderr: &str) -> Out {
    Out {
        exit,
        stdout: String::new(),
        stderr: stderr.to_string(),
    }
}

fn doc_exit(exits: &[i32]) -> Doc {
    Doc {
        kind: "usage".to_string(),
        exits: exits.iter().copied().collect(),
        action: "fix it".to_string(),
    }
}

#[test]
fn the_grader_goes_red_on_every_class_it_exists_for() {
    let d = doc_exit(&[1]);
    let p = probe_with(&["bad port"], &["password"]);
    let good = run_out(1, "Error: [RIVET_X_Y] bad port 9\n");
    assert!(grade("RIVET_X_Y", &d, &p, &good).is_ok());
    let uncoded = run_out(1, "Error: bad port 9\n");
    assert!(
        grade("RIVET_X_Y", &d, &p, &uncoded)
            .unwrap_err()
            .starts_with("uncoded")
    );
    let silent = run_out(1, "");
    assert!(
        grade("RIVET_X_Y", &d, &p, &silent)
            .unwrap_err()
            .starts_with("uncoded")
    );
    let other = run_out(1, "Error: [RIVET_Z] bad port 9\n");
    assert!(
        grade("RIVET_X_Y", &d, &p, &other)
            .unwrap_err()
            .starts_with("printed")
    );
    let exit = run_out(2, "Error: [RIVET_X_Y] bad port 9\n");
    assert!(
        grade("RIVET_X_Y", &d, &p, &exit)
            .unwrap_err()
            .starts_with("exit 2")
    );
    let cause = run_out(1, "Error: [RIVET_X_Y] disk full\n");
    assert!(
        grade("RIVET_X_Y", &d, &p, &cause)
            .unwrap_err()
            .contains("does not name")
    );
    let decoy = run_out(1, "Error: [RIVET_X_Y] bad port 9: Password rejected\n");
    assert!(
        grade("RIVET_X_Y", &d, &p, &decoy)
            .unwrap_err()
            .contains("another cause")
    );
    let before = run_out(
        1,
        "Error: [RIVET_A] first\nsome log\nError: [RIVET_X_Y] bad port 9\n",
    );
    assert!(
        grade("RIVET_X_Y", &d, &p, &before).is_ok(),
        "the last Error line is the refusal"
    );
}

#[test]
fn the_uncoded_grader_refuses_a_coded_message_and_a_zero_exit() {
    assert!(grade_uncoded(&run_out(2, "error: unexpected argument\n")).is_ok());
    assert!(grade_uncoded(&run_out(2, "Error: [RIVET_CLI_EXPORT_UNKNOWN] x\n")).is_err());
    assert!(grade_uncoded(&run_out(0, "")).is_err());
}

#[test]
fn the_literal_scan_sees_a_message_prefix_and_a_verify_token() {
    let lits = literal_codes();
    assert!(lits.contains("RIVET_VALUE_TOO_LARGE"), "{lits:?}");
    assert!(lits.contains("RIVET_VERIFY_PART_MISSING"), "{lits:?}");
    assert!(
        !lits.contains("RIVET_STATE_URL"),
        "an env name is not a code"
    );
}

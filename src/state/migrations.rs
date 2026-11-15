//! State-DB schema version and the SQLite / PostgreSQL migration ladders.

use rusqlite::Connection;

use crate::error::Result;

/// Current schema version — always the last entry in `MIGRATIONS`.
const SCHEMA_VERSION: i64 = MIGRATIONS[MIGRATIONS.len() - 1].0;

/// Each entry is `(version, sql)`.
const MIGRATIONS: &[(i64, &str)] = &[
    (
        1,
        "CREATE TABLE IF NOT EXISTS export_state (
            export_name TEXT PRIMARY KEY,
            last_cursor_value TEXT,
            last_run_at TEXT
        );
        CREATE TABLE IF NOT EXISTS export_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            export_name TEXT NOT NULL,
            run_at TEXT NOT NULL,
            duration_ms INTEGER NOT NULL,
            total_rows INTEGER NOT NULL,
            peak_rss_mb INTEGER,
            status TEXT NOT NULL,
            error_message TEXT,
            tuning_profile TEXT,
            format TEXT,
            mode TEXT,
            files_produced INTEGER DEFAULT 0,
            bytes_written INTEGER DEFAULT 0,
            retries INTEGER DEFAULT 0,
            validated INTEGER,
            schema_changed INTEGER,
            run_id TEXT
        );
        CREATE TABLE IF NOT EXISTS export_schema (
            export_name TEXT PRIMARY KEY,
            columns_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS file_manifest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            export_name TEXT NOT NULL,
            file_name TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            bytes INTEGER NOT NULL,
            format TEXT NOT NULL,
            compression TEXT,
            created_at TEXT NOT NULL
        );",
    ),
    (
        2,
        "CREATE TABLE IF NOT EXISTS chunk_run (
            run_id TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            plan_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            max_chunk_attempts INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_chunk_run_export_status
            ON chunk_run(export_name, status);
        CREATE TABLE IF NOT EXISTS chunk_task (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            start_key TEXT NOT NULL,
            end_key TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            rows_written INTEGER,
            file_name TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(run_id, chunk_index)
        );
        CREATE INDEX IF NOT EXISTS idx_chunk_task_run_status ON chunk_task(run_id, status);",
    ),
    (
        3,
        "CREATE INDEX IF NOT EXISTS idx_file_manifest_export ON file_manifest(export_name, id DESC);",
    ),
    (
        4,
        "CREATE TABLE IF NOT EXISTS export_progression (
            export_name TEXT PRIMARY KEY,
            last_committed_strategy TEXT,
            last_committed_cursor TEXT,
            last_committed_chunk_index INTEGER,
            last_committed_run_id TEXT,
            last_committed_at TEXT,
            last_verified_strategy TEXT,
            last_verified_cursor TEXT,
            last_verified_chunk_index INTEGER,
            last_verified_run_id TEXT,
            last_verified_at TEXT
        );",
    ),
    (
        5,
        "CREATE TABLE IF NOT EXISTS run_aggregate (
            run_aggregate_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            duration_ms INTEGER NOT NULL,
            config_path TEXT,
            parallel_mode TEXT NOT NULL,
            total_exports INTEGER NOT NULL,
            success_count INTEGER NOT NULL,
            failed_count INTEGER NOT NULL,
            skipped_count INTEGER NOT NULL,
            total_rows INTEGER NOT NULL,
            total_files INTEGER NOT NULL,
            total_bytes INTEGER NOT NULL,
            details_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_run_aggregate_finished
            ON run_aggregate(finished_at DESC);",
    ),
    (
        6,
        "CREATE TABLE IF NOT EXISTS export_shape (
            export_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            max_byte_len INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (export_name, column_name)
        );",
    ),
    (
        7,
        "CREATE TABLE IF NOT EXISTS run_journal (
            run_id TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            journal_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_run_journal_export
            ON run_journal(export_name, finished_at DESC);",
    ),
    (
        8,
        "ALTER TABLE file_manifest RENAME TO file_log;
        DROP INDEX IF EXISTS idx_file_manifest_export;
        CREATE INDEX IF NOT EXISTS idx_file_log_export ON file_log(export_name, id DESC);",
    ),
    (
        9,
        "ALTER TABLE export_metrics ADD COLUMN files_committed INTEGER;
        ALTER TABLE export_metrics ADD COLUMN reconciled INTEGER;
        ALTER TABLE export_metrics ADD COLUMN source_count INTEGER;
        ALTER TABLE export_metrics ADD COLUMN quality_passed INTEGER;
        ALTER TABLE export_metrics ADD COLUMN pg_temp_bytes_delta INTEGER;
        ALTER TABLE export_metrics ADD COLUMN batch_size INTEGER;
        ALTER TABLE export_metrics ADD COLUMN batch_size_memory_mb INTEGER;
        ALTER TABLE export_metrics ADD COLUMN skip_reason TEXT;
        ALTER TABLE export_metrics ADD COLUMN schema_fingerprint TEXT;
        ALTER TABLE export_metrics ADD COLUMN chunk_size INTEGER;
        ALTER TABLE export_metrics ADD COLUMN parallel INTEGER;
        ALTER TABLE export_metrics ADD COLUMN source_type TEXT;
        ALTER TABLE export_metrics ADD COLUMN destination_type TEXT;
        ALTER TABLE export_metrics ADD COLUMN rivet_version TEXT;",
    ),
    (
        10,
        "ALTER TABLE export_metrics ADD COLUMN longest_chunk_ms INTEGER;",
    ),
    (
        11,
        "CREATE TABLE IF NOT EXISTS export_harm (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            export_name TEXT NOT NULL,
            metric TEXT NOT NULL,
            delta INTEGER NOT NULL,
            recorded_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_export_harm_run ON export_harm(run_id);",
    ),
    (12, "ALTER TABLE export_metrics ADD COLUMN chunk_key TEXT;"),
    (
        13,
        "CREATE TABLE IF NOT EXISTS load_run (
            load_id TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            target_table TEXT NOT NULL,
            warehouse TEXT NOT NULL,
            mode TEXT NOT NULL,
            source_run_ids TEXT NOT NULL,
            rows_loaded INTEGER NOT NULL,
            status TEXT NOT NULL,
            finished_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_load_run_target
            ON load_run(target_table, finished_at DESC);
        CREATE TABLE IF NOT EXISTS loaded_source_run (
            target_table TEXT NOT NULL,
            source_run_id TEXT NOT NULL,
            load_id TEXT NOT NULL,
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (target_table, source_run_id)
        );",
    ),
    (
        14,
        "CREATE TABLE IF NOT EXISTS cdc_snapshot (
            export_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            run_id TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            PRIMARY KEY (export_name, table_name)
        );",
    ),
    (
        15,
        "UPDATE chunk_run SET status='interrupted'
             WHERE status='in_progress' AND run_id NOT IN (
               SELECT run_id FROM chunk_run c WHERE c.status='in_progress'
                 AND NOT EXISTS (
                   SELECT 1 FROM chunk_run c2
                   WHERE c2.export_name=c.export_name AND c2.status='in_progress'
                     AND (c2.created_at > c.created_at
                          OR (c2.created_at = c.created_at AND c2.run_id > c.run_id)))
             );
         CREATE UNIQUE INDEX IF NOT EXISTS idx_chunk_run_one_inprogress
             ON chunk_run(export_name) WHERE status='in_progress';",
    ),
    (
        16,
        "ALTER TABLE export_state ADD COLUMN resume_run_id TEXT;",
    ),
    (
        17,
        "CREATE TABLE IF NOT EXISTS run_status (
            run_id      TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            prefix      TEXT NOT NULL,
            status      TEXT NOT NULL,
            started_at  TEXT NOT NULL,
            finished_at TEXT
         );
         CREATE INDEX IF NOT EXISTS idx_run_status_prefix ON run_status(prefix);",
    ),
    (
        18,
        "ALTER TABLE export_metrics ADD COLUMN error_class TEXT;
         ALTER TABLE export_metrics ADD COLUMN cursor_min TEXT;
         ALTER TABLE export_metrics ADD COLUMN cursor_max TEXT;
         ALTER TABLE export_metrics ADD COLUMN key_descriptor_json TEXT;
         ALTER TABLE export_metrics ADD COLUMN offending_value TEXT;
         ALTER TABLE export_metrics ADD COLUMN server_context_json TEXT;",
    ),
    (
        19,
        "CREATE TABLE IF NOT EXISTS keyset_range (
            export_name TEXT NOT NULL,
            run_id      TEXT NOT NULL,
            range_index INTEGER NOT NULL,
            lo          TEXT,
            hi          TEXT,
            done        INTEGER NOT NULL DEFAULT 0,
            updated_at  TEXT NOT NULL,
            PRIMARY KEY (export_name, range_index)
        );",
    ),
    (
        20,
        "ALTER TABLE loaded_source_run ADD COLUMN source_ident TEXT;",
    ),
    (
        21,
        "DELETE FROM export_metrics WHERE status = 'running' AND id NOT IN (
             SELECT max(id) FROM export_metrics WHERE status = 'running' GROUP BY run_id
         );
         CREATE UNIQUE INDEX IF NOT EXISTS export_metrics_one_running_per_run
             ON export_metrics(run_id) WHERE status = 'running';",
    ),
    (
        22,
        "CREATE TABLE IF NOT EXISTS strategy_snapshot (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             export_name TEXT NOT NULL,
             source_schema TEXT,
             source_table TEXT NOT NULL,
             row_estimate BIGINT,
             total_bytes BIGINT,
             avg_row_bytes BIGINT,
             chosen_mode TEXT NOT NULL,
             strategy_kind TEXT,
             key_column TEXT,
             chunk_size BIGINT,
             rivet_version TEXT NOT NULL,
             captured_at TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_strategy_snapshot_export
             ON strategy_snapshot(export_name, id DESC);",
    ),
    (
        23,
        "ALTER TABLE export_metrics ADD COLUMN bytes_read INTEGER;",
    ),
    (
        24,
        "ALTER TABLE strategy_snapshot ADD COLUMN catalog_rows INTEGER;
        ALTER TABLE strategy_snapshot ADD COLUMN density REAL;
        ALTER TABLE strategy_snapshot ADD COLUMN estimate_method TEXT;
        ALTER TABLE strategy_snapshot ADD COLUMN probe_k INTEGER;
        ALTER TABLE strategy_snapshot ADD COLUMN probe_w INTEGER;",
    ),
    (25, "ALTER TABLE file_log ADD COLUMN cursor_high TEXT;"),
    (
        26,
        "ALTER TABLE export_state ADD COLUMN cursor_column TEXT;",
    ),
    (
        27,
        "CREATE TABLE IF NOT EXISTS export_load_spec (
             export_name TEXT NOT NULL,
             unit TEXT NOT NULL DEFAULT '',
             columns_json TEXT,
             primary_key_json TEXT,
             key_origin TEXT,
             run_id TEXT,
             origin TEXT NOT NULL,
             captured_at TEXT NOT NULL,
             PRIMARY KEY (export_name, unit)
         );",
    ),
    (
        28,
        "CREATE TABLE IF NOT EXISTS export_load_spec_run (
             export_name TEXT NOT NULL,
             unit TEXT NOT NULL DEFAULT '',
             run_id TEXT NOT NULL,
             columns_json TEXT NOT NULL,
             primary_key_json TEXT,
             captured_at TEXT NOT NULL,
             PRIMARY KEY (export_name, unit, run_id)
         );",
    ),
    (
        29,
        "ALTER TABLE cdc_snapshot RENAME TO cdc_snapshot_v28;
        CREATE TABLE cdc_snapshot (
            export_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            prefix TEXT NOT NULL DEFAULT '',
            run_id TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            PRIMARY KEY (export_name, table_name, prefix)
        );
        INSERT INTO cdc_snapshot (export_name, table_name, prefix, run_id, completed_at)
            SELECT export_name, table_name, '', run_id, completed_at FROM cdc_snapshot_v28;
        DROP TABLE cdc_snapshot_v28;
        ALTER TABLE keyset_range ADD COLUMN key_column TEXT;",
    ),
    (
        30,
        "ALTER TABLE export_state RENAME TO export_state_v29;
        CREATE TABLE export_state (
            export_name TEXT NOT NULL,
            prefix TEXT NOT NULL DEFAULT '',
            last_cursor_value TEXT,
            last_run_at TEXT,
            resume_run_id TEXT,
            cursor_column TEXT,
            PRIMARY KEY (export_name, prefix)
        );
        INSERT INTO export_state (export_name, prefix, last_cursor_value, last_run_at, resume_run_id, cursor_column)
            SELECT export_name, '', last_cursor_value, last_run_at, resume_run_id, cursor_column FROM export_state_v29;
        DROP TABLE export_state_v29;",
    ),
    (
        31,
        "CREATE TABLE IF NOT EXISTS state_lease (
            lease_key TEXT PRIMARY KEY,
            holder TEXT NOT NULL,
            expires_at TEXT NOT NULL
        );",
    ),
    (
        32,
        "CREATE TABLE load_spec_version (
            spec_id INTEGER PRIMARY KEY AUTOINCREMENT,
            export_name TEXT NOT NULL,
            unit TEXT NOT NULL DEFAULT '',
            columns_json TEXT NOT NULL,
            primary_key_json TEXT,
            captured_at TEXT NOT NULL
        );
        CREATE INDEX idx_load_spec_version_unit ON load_spec_version(export_name, unit);
        INSERT INTO load_spec_version (export_name, unit, columns_json, primary_key_json, captured_at)
            SELECT export_name, unit, columns_json, primary_key_json, MIN(captured_at)
            FROM export_load_spec_run
            GROUP BY export_name, unit, columns_json, primary_key_json;
        ALTER TABLE export_load_spec_run RENAME TO export_load_spec_run_v31;
        CREATE TABLE export_load_spec_run (
            export_name TEXT NOT NULL,
            unit TEXT NOT NULL DEFAULT '',
            run_id TEXT NOT NULL,
            spec_id INTEGER NOT NULL,
            captured_at TEXT NOT NULL,
            PRIMARY KEY (export_name, unit, run_id)
        );
        INSERT INTO export_load_spec_run (export_name, unit, run_id, spec_id, captured_at)
            SELECT r.export_name, r.unit, r.run_id, v.spec_id, r.captured_at
            FROM export_load_spec_run_v31 r JOIN load_spec_version v
              ON v.export_name = r.export_name AND v.unit = r.unit
             AND v.columns_json = r.columns_json AND v.primary_key_json IS r.primary_key_json;
        DROP TABLE export_load_spec_run_v31;
        CREATE INDEX IF NOT EXISTS idx_file_log_run ON file_log(run_id, file_name);
        CREATE INDEX IF NOT EXISTS idx_export_metrics_export ON export_metrics(export_name, id DESC);",
    ),
    (33, "ALTER TABLE export_state ADD COLUMN stream TEXT;"),
    (
        34,
        "ALTER TABLE export_state ADD COLUMN resume_owner TEXT;
        ALTER TABLE export_state ADD COLUMN source_schema TEXT;
        ALTER TABLE export_state ADD COLUMN population TEXT;
        ALTER TABLE export_state ADD COLUMN destination TEXT;
        ALTER TABLE chunk_run ADD COLUMN source TEXT;
        DROP INDEX IF EXISTS idx_chunk_run_one_inprogress;
        CREATE UNIQUE INDEX idx_chunk_run_one_inprogress
            ON chunk_run(export_name, COALESCE(source, '')) WHERE status='in_progress';
        ALTER TABLE keyset_range RENAME TO keyset_range_v33;
        CREATE TABLE keyset_range (
            export_name TEXT NOT NULL,
            source      TEXT,
            run_id      TEXT NOT NULL,
            range_index INTEGER NOT NULL,
            lo          TEXT,
            hi          TEXT,
            done        INTEGER NOT NULL DEFAULT 0,
            updated_at  TEXT NOT NULL,
            key_column  TEXT
        );
        INSERT INTO keyset_range (export_name, run_id, range_index, lo, hi, done, updated_at, key_column)
            SELECT export_name, run_id, range_index, lo, hi, done, updated_at, key_column FROM keyset_range_v33;
        DROP TABLE keyset_range_v33;
        CREATE UNIQUE INDEX idx_keyset_range_stream
            ON keyset_range(export_name, COALESCE(source, ''), range_index);",
    ),
    (35, "ALTER TABLE keyset_range ADD COLUMN max_key TEXT;"),
    (36, "ALTER TABLE run_status ADD COLUMN source TEXT;"),
    (37, "ALTER TABLE run_status ADD COLUMN plan_id TEXT;"),
    (
        38,
        "ALTER TABLE export_load_spec RENAME TO export_load_spec_v37;
        CREATE TABLE export_load_spec (
             export_name TEXT NOT NULL,
             unit TEXT NOT NULL DEFAULT '',
             source TEXT NOT NULL DEFAULT '',
             columns_json TEXT,
             primary_key_json TEXT,
             key_origin TEXT,
             run_id TEXT,
             origin TEXT NOT NULL,
             captured_at TEXT NOT NULL,
             PRIMARY KEY (export_name, unit, source)
         );
        INSERT INTO export_load_spec
            (export_name, unit, source, columns_json, primary_key_json, key_origin, run_id, origin, captured_at)
            SELECT export_name, unit, '', columns_json, primary_key_json, key_origin, run_id, origin, captured_at
            FROM export_load_spec_v37;
        DROP TABLE export_load_spec_v37;",
    ),
    (
        39,
        "CREATE TABLE IF NOT EXISTS run_shape (
             run_id TEXT PRIMARY KEY,
             shape_json TEXT NOT NULL,
             recorded_at TEXT NOT NULL
         );",
    ),
];

/// PostgreSQL-compatible DDL.
const PG_MIGRATIONS: &[(i64, &str)] = &[
    (
        1,
        "CREATE TABLE IF NOT EXISTS export_state (
            export_name TEXT PRIMARY KEY,
            last_cursor_value TEXT,
            last_run_at TEXT
        );
        CREATE TABLE IF NOT EXISTS export_metrics (
            id BIGSERIAL PRIMARY KEY,
            export_name TEXT NOT NULL,
            run_at TEXT NOT NULL,
            duration_ms BIGINT NOT NULL,
            total_rows BIGINT NOT NULL,
            peak_rss_mb BIGINT,
            status TEXT NOT NULL,
            error_message TEXT,
            tuning_profile TEXT,
            format TEXT,
            mode TEXT,
            files_produced BIGINT DEFAULT 0,
            bytes_written BIGINT DEFAULT 0,
            retries BIGINT DEFAULT 0,
            validated BOOLEAN,
            schema_changed BOOLEAN,
            run_id TEXT
        );
        CREATE TABLE IF NOT EXISTS export_schema (
            export_name TEXT PRIMARY KEY,
            columns_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS file_manifest (
            id BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            export_name TEXT NOT NULL,
            file_name TEXT NOT NULL,
            row_count BIGINT NOT NULL,
            bytes BIGINT NOT NULL,
            format TEXT NOT NULL,
            compression TEXT,
            created_at TEXT NOT NULL
        );",
    ),
    (
        2,
        "CREATE TABLE IF NOT EXISTS chunk_run (
            run_id TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            plan_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            max_chunk_attempts BIGINT NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_chunk_run_export_status
            ON chunk_run(export_name, status);
        CREATE TABLE IF NOT EXISTS chunk_task (
            id BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            chunk_index BIGINT NOT NULL,
            start_key TEXT NOT NULL,
            end_key TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts BIGINT NOT NULL DEFAULT 0,
            last_error TEXT,
            rows_written BIGINT,
            file_name TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(run_id, chunk_index)
        );
        CREATE INDEX IF NOT EXISTS idx_chunk_task_run_status ON chunk_task(run_id, status);",
    ),
    (
        3,
        "CREATE INDEX IF NOT EXISTS idx_file_manifest_export ON file_manifest(export_name, id DESC);",
    ),
    (
        4,
        "CREATE TABLE IF NOT EXISTS export_progression (
            export_name TEXT PRIMARY KEY,
            last_committed_strategy TEXT,
            last_committed_cursor TEXT,
            last_committed_chunk_index BIGINT,
            last_committed_run_id TEXT,
            last_committed_at TEXT,
            last_verified_strategy TEXT,
            last_verified_cursor TEXT,
            last_verified_chunk_index BIGINT,
            last_verified_run_id TEXT,
            last_verified_at TEXT
        );",
    ),
    (
        5,
        "CREATE TABLE IF NOT EXISTS run_aggregate (
            run_aggregate_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            duration_ms BIGINT NOT NULL,
            config_path TEXT,
            parallel_mode TEXT NOT NULL,
            total_exports BIGINT NOT NULL,
            success_count BIGINT NOT NULL,
            failed_count BIGINT NOT NULL,
            skipped_count BIGINT NOT NULL,
            total_rows BIGINT NOT NULL,
            total_files BIGINT NOT NULL,
            total_bytes BIGINT NOT NULL,
            details_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_run_aggregate_finished
            ON run_aggregate(finished_at DESC);",
    ),
    (
        6,
        "CREATE TABLE IF NOT EXISTS export_shape (
            export_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            max_byte_len BIGINT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (export_name, column_name)
        );",
    ),
    (
        7,
        "CREATE TABLE IF NOT EXISTS run_journal (
            run_id TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            journal_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_run_journal_export
            ON run_journal(export_name, finished_at DESC);",
    ),
    (
        8,
        "ALTER TABLE file_manifest RENAME TO file_log;
        DROP INDEX IF EXISTS idx_file_manifest_export;
        CREATE INDEX IF NOT EXISTS idx_file_log_export ON file_log(export_name, id DESC);",
    ),
    (
        9,
        "ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS files_committed BIGINT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS reconciled BOOLEAN;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS source_count BIGINT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS quality_passed BOOLEAN;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS pg_temp_bytes_delta BIGINT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS batch_size BIGINT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS batch_size_memory_mb BIGINT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS skip_reason TEXT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS schema_fingerprint TEXT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS chunk_size BIGINT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS parallel BIGINT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS source_type TEXT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS destination_type TEXT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS rivet_version TEXT;",
    ),
    (
        10,
        "ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS longest_chunk_ms BIGINT;",
    ),
    (
        11,
        "CREATE TABLE IF NOT EXISTS export_harm (
            id BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            export_name TEXT NOT NULL,
            metric TEXT NOT NULL,
            delta BIGINT NOT NULL,
            recorded_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_export_harm_run ON export_harm(run_id);",
    ),
    (
        12,
        "ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS chunk_key TEXT;",
    ),
    (
        13,
        "CREATE TABLE IF NOT EXISTS load_run (
            load_id TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            target_table TEXT NOT NULL,
            warehouse TEXT NOT NULL,
            mode TEXT NOT NULL,
            source_run_ids TEXT NOT NULL,
            rows_loaded BIGINT NOT NULL,
            status TEXT NOT NULL,
            finished_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_load_run_target
            ON load_run(target_table, finished_at DESC);
        CREATE TABLE IF NOT EXISTS loaded_source_run (
            target_table TEXT NOT NULL,
            source_run_id TEXT NOT NULL,
            load_id TEXT NOT NULL,
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (target_table, source_run_id)
        );",
    ),
    (
        14,
        "CREATE TABLE IF NOT EXISTS cdc_snapshot (
            export_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            run_id TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            PRIMARY KEY (export_name, table_name)
        );",
    ),
    (
        15,
        "UPDATE chunk_run SET status='interrupted'
             WHERE status='in_progress' AND run_id NOT IN (
               SELECT run_id FROM chunk_run c WHERE c.status='in_progress'
                 AND NOT EXISTS (
                   SELECT 1 FROM chunk_run c2
                   WHERE c2.export_name=c.export_name AND c2.status='in_progress'
                     AND (c2.created_at > c.created_at
                          OR (c2.created_at = c.created_at AND c2.run_id > c.run_id)))
             );
         CREATE UNIQUE INDEX IF NOT EXISTS idx_chunk_run_one_inprogress
             ON chunk_run(export_name) WHERE status='in_progress';",
    ),
    (
        16,
        "ALTER TABLE export_state ADD COLUMN IF NOT EXISTS resume_run_id TEXT;",
    ),
    (
        17,
        "CREATE TABLE IF NOT EXISTS run_status (
            run_id      TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            prefix      TEXT NOT NULL,
            status      TEXT NOT NULL,
            started_at  TEXT NOT NULL,
            finished_at TEXT
         );
         CREATE INDEX IF NOT EXISTS idx_run_status_prefix ON run_status(prefix);",
    ),
    (
        18,
        "ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS error_class TEXT;
         ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS cursor_min TEXT;
         ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS cursor_max TEXT;
         ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS key_descriptor_json TEXT;
         ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS offending_value TEXT;
         ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS server_context_json TEXT;",
    ),
    (
        19,
        "CREATE TABLE IF NOT EXISTS keyset_range (
            export_name TEXT NOT NULL,
            run_id      TEXT NOT NULL,
            range_index BIGINT NOT NULL,
            lo          TEXT,
            hi          TEXT,
            done        BIGINT NOT NULL DEFAULT 0,
            updated_at  TEXT NOT NULL,
            PRIMARY KEY (export_name, range_index)
        );",
    ),
    (
        20,
        "ALTER TABLE loaded_source_run ADD COLUMN IF NOT EXISTS source_ident TEXT;",
    ),
    (
        21,
        "DELETE FROM export_metrics a
             WHERE a.status = 'running'
               AND a.id < (SELECT max(b.id) FROM export_metrics b
                            WHERE b.run_id = a.run_id AND b.status = 'running');
         CREATE UNIQUE INDEX IF NOT EXISTS export_metrics_one_running_per_run
             ON export_metrics(run_id) WHERE status = 'running';",
    ),
    (
        22,
        "CREATE TABLE IF NOT EXISTS strategy_snapshot (
             id BIGSERIAL PRIMARY KEY,
             export_name TEXT NOT NULL,
             source_schema TEXT,
             source_table TEXT NOT NULL,
             row_estimate BIGINT,
             total_bytes BIGINT,
             avg_row_bytes BIGINT,
             chosen_mode TEXT NOT NULL,
             strategy_kind TEXT,
             key_column TEXT,
             chunk_size BIGINT,
             rivet_version TEXT NOT NULL,
             captured_at TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_strategy_snapshot_export
             ON strategy_snapshot(export_name, id DESC);",
    ),
    (
        23,
        "ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS bytes_read BIGINT;",
    ),
    (
        24,
        "ALTER TABLE strategy_snapshot ADD COLUMN IF NOT EXISTS catalog_rows BIGINT;
        ALTER TABLE strategy_snapshot ADD COLUMN IF NOT EXISTS density DOUBLE PRECISION;
        ALTER TABLE strategy_snapshot ADD COLUMN IF NOT EXISTS estimate_method TEXT;
        ALTER TABLE strategy_snapshot ADD COLUMN IF NOT EXISTS probe_k BIGINT;
        ALTER TABLE strategy_snapshot ADD COLUMN IF NOT EXISTS probe_w BIGINT;",
    ),
    (
        25,
        "ALTER TABLE file_log ADD COLUMN IF NOT EXISTS cursor_high TEXT;",
    ),
    (
        26,
        "ALTER TABLE export_state ADD COLUMN IF NOT EXISTS cursor_column TEXT;",
    ),
    (
        27,
        "CREATE TABLE IF NOT EXISTS export_load_spec (
             export_name TEXT NOT NULL,
             unit TEXT NOT NULL DEFAULT '',
             columns_json TEXT,
             primary_key_json TEXT,
             key_origin TEXT,
             run_id TEXT,
             origin TEXT NOT NULL,
             captured_at TEXT NOT NULL,
             PRIMARY KEY (export_name, unit)
         );",
    ),
    (
        28,
        "CREATE TABLE IF NOT EXISTS export_load_spec_run (
             export_name TEXT NOT NULL,
             unit TEXT NOT NULL DEFAULT '',
             run_id TEXT NOT NULL,
             columns_json TEXT NOT NULL,
             primary_key_json TEXT,
             captured_at TEXT NOT NULL,
             PRIMARY KEY (export_name, unit, run_id)
         );",
    ),
    (
        29,
        "ALTER TABLE cdc_snapshot ADD COLUMN IF NOT EXISTS prefix TEXT NOT NULL DEFAULT '';
         ALTER TABLE cdc_snapshot DROP CONSTRAINT IF EXISTS cdc_snapshot_pkey;
         ALTER TABLE cdc_snapshot ADD PRIMARY KEY (export_name, table_name, prefix);
         ALTER TABLE keyset_range ADD COLUMN IF NOT EXISTS key_column TEXT;",
    ),
    (
        30,
        "ALTER TABLE export_state ADD COLUMN IF NOT EXISTS prefix TEXT NOT NULL DEFAULT '';
         ALTER TABLE export_state DROP CONSTRAINT IF EXISTS export_state_pkey;
         ALTER TABLE export_state ADD PRIMARY KEY (export_name, prefix);",
    ),
    (
        31,
        "CREATE TABLE IF NOT EXISTS state_lease (
            lease_key TEXT PRIMARY KEY,
            holder TEXT NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL
        );",
    ),
    (
        32,
        "CREATE TABLE IF NOT EXISTS load_spec_version (
            spec_id BIGSERIAL PRIMARY KEY,
            export_name TEXT NOT NULL,
            unit TEXT NOT NULL DEFAULT '',
            columns_json TEXT NOT NULL,
            primary_key_json TEXT,
            captured_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_load_spec_version_unit ON load_spec_version(export_name, unit);
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM information_schema.columns
                     WHERE table_schema = current_schema()
                       AND table_name = 'export_load_spec_run' AND column_name = 'columns_json') THEN
            INSERT INTO load_spec_version (export_name, unit, columns_json, primary_key_json, captured_at)
                SELECT export_name, unit, columns_json, primary_key_json, MIN(captured_at)
                FROM export_load_spec_run
                GROUP BY export_name, unit, columns_json, primary_key_json;
            ALTER TABLE export_load_spec_run ADD COLUMN IF NOT EXISTS spec_id BIGINT;
            UPDATE export_load_spec_run r SET spec_id = v.spec_id FROM load_spec_version v
                WHERE v.export_name = r.export_name AND v.unit = r.unit
                  AND v.columns_json = r.columns_json
                  AND v.primary_key_json IS NOT DISTINCT FROM r.primary_key_json;
            ALTER TABLE export_load_spec_run ALTER COLUMN spec_id SET NOT NULL;
            ALTER TABLE export_load_spec_run DROP COLUMN columns_json;
            ALTER TABLE export_load_spec_run DROP COLUMN IF EXISTS primary_key_json;
          END IF;
        END $$;
        CREATE INDEX IF NOT EXISTS idx_file_log_run ON file_log(run_id, file_name);
        CREATE INDEX IF NOT EXISTS idx_export_metrics_export ON export_metrics(export_name, id DESC);",
    ),
    (
        33,
        "ALTER TABLE export_state ADD COLUMN IF NOT EXISTS stream TEXT;",
    ),
    (
        34,
        "ALTER TABLE export_state ADD COLUMN IF NOT EXISTS resume_owner TEXT;
        ALTER TABLE export_state ADD COLUMN IF NOT EXISTS source_schema TEXT;
        ALTER TABLE export_state ADD COLUMN IF NOT EXISTS population TEXT;
        ALTER TABLE export_state ADD COLUMN IF NOT EXISTS destination TEXT;
        ALTER TABLE chunk_run ADD COLUMN IF NOT EXISTS source TEXT;
        DROP INDEX IF EXISTS idx_chunk_run_one_inprogress;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_chunk_run_one_inprogress
            ON chunk_run(export_name, COALESCE(source, '')) WHERE status='in_progress';
        ALTER TABLE keyset_range ADD COLUMN IF NOT EXISTS source TEXT;
        ALTER TABLE keyset_range DROP CONSTRAINT IF EXISTS keyset_range_pkey;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_keyset_range_stream
            ON keyset_range(export_name, COALESCE(source, ''), range_index);",
    ),
    (
        35,
        "ALTER TABLE keyset_range ADD COLUMN IF NOT EXISTS max_key TEXT;",
    ),
    (
        36,
        "ALTER TABLE run_status ADD COLUMN IF NOT EXISTS source TEXT;",
    ),
    (
        37,
        "ALTER TABLE run_status ADD COLUMN IF NOT EXISTS plan_id TEXT;",
    ),
    (
        38,
        "ALTER TABLE export_load_spec ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT '';
        ALTER TABLE export_load_spec DROP CONSTRAINT IF EXISTS export_load_spec_pkey;
        ALTER TABLE export_load_spec ADD CONSTRAINT export_load_spec_pkey PRIMARY KEY (export_name, unit, source);",
    ),
    (
        39,
        "CREATE TABLE IF NOT EXISTS run_shape (
             run_id TEXT PRIMARY KEY,
             shape_json TEXT NOT NULL,
             recorded_at TEXT NOT NULL
         );",
    ),
];

/// Where the restore procedure is written.
const RESTORE_DOC: &str = "docs/reference/state.md, section \"Rolling back an upgrade\"";

/// Whether a state at `current` is about to be migrated by this build: a copy is written first.
fn needs_copy(current: i64) -> bool {
    current > 0 && current < SCHEMA_VERSION
}

/// The pre-migration copy of a SQLite state file: `<state>.v<old>.<UTC stamp>.bak` beside it.
fn backup_path(db: &str, old: i64) -> std::path::PathBuf {
    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    std::path::PathBuf::from(format!("{db}.v{old}.{stamp}.bak"))
}

/// Write a consistent copy of the state file beside it through a second connection (`VACUUM INTO` and the backup API both refuse a connection holding the write transaction).
fn copy_state_before_migration(conn: &Connection, old: i64) -> Result<Option<std::path::PathBuf>> {
    let Some(db) = conn.path().filter(|p| !p.is_empty()) else {
        return Ok(None);
    };
    let copy = backup_path(db, old);
    let write = || -> std::result::Result<(), String> {
        let open = |p: &std::path::Path| Connection::open(p).map_err(|e| e.to_string());
        open(std::path::Path::new(db))?
            .execute_batch(&format!(
                "VACUUM INTO '{}';",
                copy.display().to_string().replace('\'', "''")
            ))
            .map_err(|e| e.to_string())?;
        let ver: i64 = open(&copy)?
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_version",
                [],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        if ver != old {
            return Err(format!("the copy reads schema v{ver}, the state is v{old}"));
        }
        Ok(())
    };
    if let Err(e) = write() {
        let _ = std::fs::remove_file(&copy);
        return Err(crate::error::coded(
            crate::error::codes::STATE_BACKUP_FAILED,
            format!(
                "state: could not write a copy of the v{old} state at `{}` before migrating it to \
                 v{SCHEMA_VERSION}: {e}. Nothing was migrated; the state is as the previous rivet \
                 left it. Free the space or make the directory writable and run again, or keep \
                 running the previous release.",
                copy.display()
            ),
        ));
    }
    Ok(Some(copy))
}

/// Refuse to migrate a v`old` state while `held`, a lease on it, is another live process's `(lease, holder)`.
fn refuse_beside_a_live_lease(old: i64, held: Option<(String, String)>) -> Result<()> {
    let Some((lease, holder)) = seeded!("migration-beside-a-live-lease", None, held) else {
        return Ok(());
    };
    crate::rivet_bail!(
        crate::error::codes::STATE_UPGRADE_BESIDE_LIVE_RUN,
        "state: this state is at schema v{old} and its lease `{lease}` (the run or load of the \
         export or table it names) is held by {holder}, a live rivet whose release the lease does \
         not record: migrating the state to v{SCHEMA_VERSION} now would change its schema under \
         that run, and an older release cannot open a v{SCHEMA_VERSION} state. Nothing was \
         migrated and nothing was written. Stop or wait for that run of the older release, then \
         run again, or give `--lock-wait` the seconds it needs."
    )
}

/// The lease beside the SQLite state of `conn` that another live process holds, if any.
fn sqlite_lease_held(conn: &Connection) -> Result<Option<(String, String)>> {
    match conn.path().filter(|p| !p.is_empty()) {
        Some(db) => super::load_lease::sqlite_lease_held(std::path::Path::new(db))
            .map_err(|e| leases_unreadable(db, &e)),
        None => Ok(None),
    }
}

/// Wait up to `wait` while `held` answers a lease another live process keeps on the state.
fn wait_while_a_lease_is_held(
    wait: std::time::Duration,
    mut held: impl FnMut() -> Result<Option<(String, String)>>,
) -> Result<()> {
    use super::load_lease::{WAIT_NOTE_EVERY, WAIT_POLL, note_due};
    let started = std::time::Instant::now();
    let mut noted: Option<std::time::Instant> = None;
    while let Some((lease, holder)) = held()? {
        let waited = started.elapsed();
        if waited >= wait {
            break;
        }
        if note_due(noted, WAIT_NOTE_EVERY) {
            log::warn!(
                "state: its lease `{lease}` is held by {holder}: waiting up to {} s for it before \
                 migrating the state to v{SCHEMA_VERSION} (`--lock-wait`)",
                wait.as_secs()
            );
            noted = Some(std::time::Instant::now());
        }
        std::thread::sleep(WAIT_POLL.min(wait - waited));
    }
    Ok(())
}

/// The leases of the state `at` could not be read before a migration; nothing was migrated.
fn leases_unreadable(at: &str, e: &dyn std::fmt::Display) -> anyhow::Error {
    crate::error::coded(
        crate::error::codes::STATE_BACKEND_FAILED,
        format!(
            "state: could not read the leases of `{at}` before migrating it to v{SCHEMA_VERSION} \
             ({e}). Nothing was migrated; the state is as the previous rivet left it."
        ),
    )
}

/// The table names a ladder's `CREATE TABLE` statements define (`IF NOT EXISTS` stripped).
fn created_table_names(sql: &str) -> std::collections::BTreeSet<String> {
    let lower = sql.to_lowercase();
    lower
        .match_indices("create table")
        .filter_map(|(i, kw)| {
            let after = lower[i + kw.len()..].trim_start();
            let after = after
                .strip_prefix("if not exists")
                .map_or(after, str::trim_start);
            let name: String = after
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            (!name.is_empty()).then_some(name)
        })
        .collect()
}

/// The ladder step with its `DO $$` guard removed, so a replay runs the body again.
#[cfg(feature = "seeded")]
fn unguarded(sql: &str) -> String {
    let (Some(start), Some(then), Some(end_if), Some(end)) = (
        sql.find("DO $$ BEGIN"),
        sql.find("THEN\n"),
        sql.find("END IF;"),
        sql.find("END $$;"),
    ) else {
        return sql.to_string();
    };
    let body = &sql[then + "THEN\n".len()..end_if];
    format!("{}{}{}", &sql[..start], body, &sql[end + "END $$;".len()..])
}

/// Every table the PostgreSQL ladder creates or renames to, plus the version table.
fn pg_state_table_names() -> Vec<String> {
    let mut out = std::collections::BTreeSet::from(["rivet_schema_version".to_string()]);
    for &(_, sql) in PG_MIGRATIONS {
        out.extend(created_table_names(sql));
        let lower = sql.to_lowercase();
        for (i, kw) in lower.match_indices("rename to ") {
            let name: String = lower[i + kw.len()..]
                .trim_start()
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                out.insert(name);
            }
        }
    }
    out.into_iter().collect()
}

/// The schema holding the pre-migration copy of a v`old` PostgreSQL state.
fn pg_backup_schema(old: i64) -> String {
    format!("rivet_state_backup_v{old}")
}

/// Copy every state table into `rivet_state_backup_v<old>` inside the migration transaction; an existing copy is kept, a copy that cannot be written aborts the migration.
fn copy_pg_state_before_migration(client: &mut postgres::Transaction<'_>, old: i64) -> Result<()> {
    let schema = pg_backup_schema(old);
    let failed = |what: &str, e: &postgres::Error| {
        crate::error::coded(
            crate::error::codes::STATE_BACKUP_FAILED,
            format!(
                "state(pg): could not write a copy of the v{old} state into schema `{schema}` \
                 before migrating it to v{SCHEMA_VERSION} ({what}: {}). Nothing was migrated; \
                 the state is as the previous rivet left it. Give the state role CREATE on the \
                 database (or free the space) and run again, or keep running the previous release.",
                super::pg_detail(e)
            ),
        )
    };
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)",
            &[&schema],
        )
        .map_err(|e| failed("look for an earlier copy", &e))?
        .get(0);
    if exists {
        log::warn!(
            "state(pg): upgrading the state schema v{old} -> v{SCHEMA_VERSION}; schema `{schema}` \
             already holds a copy of a v{old} state from an earlier attempt and is kept as it is — \
             to go back to the previous rivet release, see {RESTORE_DOC}"
        );
        return Ok(());
    }
    let tables: Vec<String> = client
        .query(
            "SELECT tablename::text FROM pg_tables \
             WHERE schemaname = current_schema() AND tablename::text = ANY($1::text[]) ORDER BY 1",
            &[&pg_state_table_names()],
        )
        .map_err(|e| failed("list the state tables", &e))?
        .iter()
        .map(|r| r.get(0))
        .collect();
    let serials: Vec<(String, String, String)> = client
        .query(
            "SELECT c.relname::text, a.attname::text, \
                    pg_get_serial_sequence(format('%I.%I', n.nspname, c.relname), a.attname::text) \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = current_schema() AND c.relname::text = ANY($1::text[]) \
               AND a.attnum > 0 AND NOT a.attisdropped \
               AND pg_get_serial_sequence(format('%I.%I', n.nspname, c.relname), a.attname::text) \
                   IS NOT NULL",
            &[&tables],
        )
        .map_err(|e| failed("list the serial columns", &e))?
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    let mut sql = format!("CREATE SCHEMA {schema};");
    for t in &tables {
        sql.push_str(&format!(
            " CREATE TABLE {schema}.{t} (LIKE {t} INCLUDING ALL); \
             INSERT INTO {schema}.{t} SELECT * FROM {t};"
        ));
    }
    for (t, col, seq) in &serials {
        let own = format!("{schema}.{t}_{col}_seq");
        sql.push_str(&format!(
            " CREATE SEQUENCE {own} OWNED BY {schema}.{t}.{col}; \
             SELECT setval('{own}', last_value, is_called) FROM {seq}; \
             ALTER TABLE {schema}.{t} ALTER COLUMN {col} SET DEFAULT nextval('{own}');"
        ));
    }
    client.batch_execute(&sql).map_err(|e| failed("copy", &e))?;
    log::warn!(
        "state(pg): upgrading the state schema v{old} -> v{SCHEMA_VERSION}; a copy of the v{old} \
         state ({} tables) is in schema `{schema}` — to go back to the previous rivet release, \
         see {RESTORE_DOC}",
        tables.len()
    );
    Ok(())
}

fn ensure_schema_version_table(conn: &Connection) {
    let _ = conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL
        );",
    );
}

fn get_current_version(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_version",
        [],
        |row| row.get(0),
    )
    .unwrap_or(0)
}

/// Whether a version read WITHOUT the migration lock already equals this build's schema.
fn already_current(unlocked_version: i64) -> bool {
    unlocked_version == SCHEMA_VERSION
}

pub(super) fn migrate(conn: &Connection) -> Result<()> {
    let unlocked = get_current_version(conn);
    if already_current(unlocked) {
        return Ok(());
    }
    if needs_copy(unlocked) {
        wait_while_a_lease_is_held(std::time::Duration::from_secs(super::lock_wait_s()), || {
            sqlite_lease_held(conn)
        })?;
    }
    conn.execute_batch("BEGIN IMMEDIATE;")
        .map_err(|e| write_lock_not_taken(conn.path(), &e))?;
    let out = migrate_locked(conn);
    let _ = conn.execute_batch(if out.is_ok() { "COMMIT;" } else { "ROLLBACK;" });
    out
}

/// Why `BEGIN IMMEDIATE` did not take the write lock.
fn write_lock_not_taken(db: Option<&str>, e: &rusqlite::Error) -> anyhow::Error {
    let msg = e.to_string();
    if msg.contains("file is not a database") || msg.contains("database disk image") {
        return crate::error::coded(
            crate::error::codes::STATE_FILE_CORRUPT,
            format!(
                "state: {msg} — the state DB file is CORRUPT (or not a SQLite file at \
             all), not locked. Move it aside and re-run (rivet rebuilds state; \
             incremental cursors and skip-ledgers start fresh), or restore it from \
             a backup."
            ),
        );
    }
    if seeded!("readonly-state-read-as-migration-lock", false, true)
        && e.sqlite_error_code() == Some(rusqlite::ErrorCode::ReadOnly)
    {
        let at = db.map_or(String::new(), |path| format!(" `{path}`"));
        return anyhow::Error::new(crate::error::CodedError::new(
            crate::error::codes::STATE_NOT_WRITABLE,
            format!(
                "state: the state database{at} is read-only ({e}). SQLite writes the file and, \
                 beside it, its `-wal` and `-shm` files, so the file and its directory must \
                 both be writable. The state was not opened and nothing was written. Make the \
                 state database writable and run again."
            ),
        ));
    }
    crate::error::coded(
        crate::error::codes::STATE_MIGRATION_LOCKED,
        format!(
            "state: could not acquire the migration lock within the busy timeout ({e}). \
         Another rivet process is migrating this state database; wait for it to \
         finish and retry. Running the migration ladder without the lock is what \
         produces 'no such table' / 'duplicate column' failures on a shared backend."
        ),
    )
}

#[cfg(test)]
mod write_lock_tests {
    use super::write_lock_not_taken;

    /// The error SQLite gives for primary or extended result code `code`.
    fn sqlite(code: std::ffi::c_int, text: &str) -> rusqlite::Error {
        rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), Some(text.to_string()))
    }

    #[test]
    fn a_read_only_database_is_refused_as_not_writable_never_as_another_process() {
        for code in [
            rusqlite::ffi::SQLITE_READONLY,
            rusqlite::ffi::SQLITE_READONLY_DIRECTORY,
            rusqlite::ffi::SQLITE_READONLY_CANTINIT,
        ] {
            let e = sqlite(code, "attempt to write a readonly database");
            let err = write_lock_not_taken(Some("/data/.rivet_state.db"), &e);
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_STATE_NOT_WRITABLE"),
                "{code}"
            );
            assert_eq!(crate::error::classify_exit(&err), 1, "{code}");
            assert_eq!(
                err.to_string(),
                "state: the state database `/data/.rivet_state.db` is read-only (attempt to write \
                 a readonly database). SQLite writes the file and, beside it, its `-wal` and \
                 `-shm` files, so the file and its directory must both be writable. The state \
                 was not opened and nothing was written. Make the state database writable and \
                 run again."
            );
        }
        let unnamed = write_lock_not_taken(
            None,
            &sqlite(
                rusqlite::ffi::SQLITE_READONLY,
                "attempt to write a readonly database",
            ),
        );
        assert!(
            unnamed
                .to_string()
                .starts_with("state: the state database is read-only ("),
            "{unnamed}"
        );
    }

    #[test]
    fn a_held_lock_and_a_corrupt_file_keep_their_own_causes() {
        let busy = write_lock_not_taken(
            Some("s.db"),
            &sqlite(rusqlite::ffi::SQLITE_BUSY, "database is locked"),
        );
        crate::error::assert_coded(&busy, "RIVET_STATE_MIGRATION_LOCKED", 2);
        assert!(
            busy.to_string()
                .contains("Another rivet process is migrating this state database"),
            "{busy}"
        );
        let corrupt = write_lock_not_taken(
            Some("s.db"),
            &sqlite(rusqlite::ffi::SQLITE_NOTADB, "file is not a database"),
        );
        crate::error::assert_coded(&corrupt, "RIVET_STATE_FILE_CORRUPT", 1);
        assert!(corrupt.to_string().contains("CORRUPT"), "{corrupt}");
        assert!(
            !corrupt.to_string().contains("Another rivet process"),
            "{corrupt}"
        );
    }
}

fn migrate_locked(conn: &Connection) -> Result<()> {
    ensure_schema_version_table(conn);

    let current = get_current_version(conn);

    if current == 0 {
        let has_export_state: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='export_state'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if has_export_state {
            let metrics_cols = [
                "files_produced INTEGER DEFAULT 0",
                "bytes_written INTEGER DEFAULT 0",
                "retries INTEGER DEFAULT 0",
                "validated INTEGER",
                "schema_changed INTEGER",
                "run_id TEXT",
            ];
            for col_def in &metrics_cols {
                let sql = format!("ALTER TABLE export_metrics ADD COLUMN {}", col_def);
                let _ = conn.execute(&sql, []);
            }
        }
    }

    if needs_copy(current) {
        refuse_beside_a_live_lease(current, sqlite_lease_held(conn)?)?;
    }
    if needs_copy(current)
        && seeded!("migration-without-a-way-back", false, true)
        && let Some(copy) = copy_state_before_migration(conn, current)?
    {
        log::warn!(
            "state: upgrading the state schema v{current} -> v{SCHEMA_VERSION}; a copy of the \
             v{current} state is at `{}` — to go back to the previous rivet release, see {RESTORE_DOC}",
            copy.display()
        );
    }

    for &(ver, sql) in MIGRATIONS {
        if ver > current {
            log::debug!("state: applying migration v{}", ver);
            let atomic_sql = format!(
                "{}\nINSERT INTO schema_version (version) VALUES ({});",
                sql, ver
            );
            conn.execute_batch(&atomic_sql).map_err(|e| {
                crate::error::coded(
                    crate::error::codes::STATE_BACKEND_FAILED,
                    format!("state: migration v{} failed: {}", ver, e),
                )
            })?;
        }
    }

    let _ = conn.execute(
        "DELETE FROM schema_version WHERE version < (SELECT MAX(version) FROM schema_version)",
        [],
    );

    let final_version = get_current_version(conn);
    if final_version > SCHEMA_VERSION {
        crate::rivet_bail!(
            crate::error::codes::STATE_SCHEMA_NEWER,
            "state: this state DB is at schema v{final_version}, newer than this rivet knows \
             (v{SCHEMA_VERSION}) — a newer rivet migrated it. Upgrade rivet, or point this one \
             at a state DB it created; a downgrade never rewrites the schema"
        );
    }
    if final_version != SCHEMA_VERSION {
        crate::rivet_bail!(
            crate::error::codes::INTERNAL_STATE_INVARIANT,
            "state: migration incomplete — expected schema v{} but reached v{}",
            SCHEMA_VERSION,
            final_version
        );
    }

    Ok(())
}

/// The advisory-lock key for state migrations.
const PG_MIGRATION_LOCK: i64 = 0x7269_7665_745f_6d69_u64 as i64;

pub(super) fn migrate_pg(client: &mut postgres::Client) -> Result<()> {
    let unlocked = client
        .query_one(
            "SELECT COALESCE(MAX(version), 0) FROM rivet_schema_version",
            &[],
        )
        .map(|r| r.get::<_, i64>(0))
        .unwrap_or(0);
    if already_current(unlocked) {
        return Ok(());
    }
    if needs_copy(unlocked) {
        wait_while_a_lease_is_held(std::time::Duration::from_secs(super::lock_wait_s()), || {
            super::load_lease::pg_lease_held(client)
                .map_err(|e| leases_unreadable("the PostgreSQL state", &super::pg_detail(&e)))
        })?;
    }
    let mut tx = client.transaction().map_err(|e| {
        crate::error::coded(
            crate::error::codes::STATE_BACKEND_FAILED,
            format!("state(pg): begin migration: {}", super::pg_detail(&e)),
        )
    })?;
    tx.batch_execute(&format!(
        "SELECT pg_advisory_xact_lock({PG_MIGRATION_LOCK});"
    ))
    .map_err(|e| {
        crate::error::coded(
            crate::error::codes::STATE_BACKEND_FAILED,
            format!("state(pg): take migration lock: {}", super::pg_detail(&e)),
        )
    })?;
    migrate_pg_locked(&mut tx)?;
    tx.commit().map_err(|e| {
        crate::error::coded(
            crate::error::codes::STATE_BACKEND_FAILED,
            format!("state(pg): commit migration: {}", super::pg_detail(&e)),
        )
    })
}

fn migrate_pg_locked(client: &mut postgres::Transaction<'_>) -> Result<()> {
    client
        .batch_execute("CREATE TABLE IF NOT EXISTS rivet_schema_version (version BIGINT NOT NULL);")
        .map_err(|e| {
            crate::error::coded(
                crate::error::codes::STATE_BACKEND_FAILED,
                format!("state(pg): create version table: {}", super::pg_detail(&e)),
            )
        })?;

    let current: i64 = client
        .query_one(
            "SELECT COALESCE(MAX(version), 0) FROM rivet_schema_version",
            &[],
        )
        .map_err(|e| {
            crate::error::coded(
                crate::error::codes::STATE_BACKEND_FAILED,
                format!("state(pg): read schema version: {}", super::pg_detail(&e)),
            )
        })?
        .get(0);

    if needs_copy(current) {
        let held = super::load_lease::pg_lease_held(client)
            .map_err(|e| leases_unreadable("the PostgreSQL state", &super::pg_detail(&e)))?;
        refuse_beside_a_live_lease(current, held)?;
        seeded!(
            "migration-without-a-way-back",
            Ok(()),
            copy_pg_state_before_migration(client, current)
        )?;
    }

    for &(ver, sql) in PG_MIGRATIONS {
        if ver > current {
            log::debug!("state(pg): applying migration v{}", ver);
            let batch = format!(
                "{} INSERT INTO rivet_schema_version (version) VALUES ({});",
                seeded!(
                    "pg-migration-not-idempotent",
                    unguarded(sql),
                    sql.to_string()
                ),
                ver
            );
            client.batch_execute(&batch).map_err(|e| {
                crate::error::coded(
                    crate::error::codes::STATE_BACKEND_FAILED,
                    format!(
                        "state(pg): migration v{} failed: {}",
                        ver,
                        super::pg_detail(&e)
                    ),
                )
            })?;
        }
    }

    client
        .batch_execute(
            "DELETE FROM rivet_schema_version \
             WHERE version < (SELECT MAX(version) FROM rivet_schema_version);",
        )
        .map_err(|e| {
            crate::error::coded(
                crate::error::codes::STATE_BACKEND_FAILED,
                format!("state(pg): prune schema versions: {}", super::pg_detail(&e)),
            )
        })?;

    let final_version: i64 = client
        .query_one(
            "SELECT COALESCE(MAX(version), 0) FROM rivet_schema_version",
            &[],
        )
        .map_err(|e| {
            crate::error::coded(
                crate::error::codes::STATE_BACKEND_FAILED,
                format!(
                    "state(pg): read final schema version: {}",
                    super::pg_detail(&e)
                ),
            )
        })?
        .get(0);
    if final_version > SCHEMA_VERSION {
        crate::rivet_bail!(
            crate::error::codes::STATE_SCHEMA_NEWER,
            "state(pg): this state DB is at schema v{final_version}, newer than this rivet knows \
             (v{SCHEMA_VERSION}) — a newer rivet migrated it. Upgrade rivet, or point this one at \
             a state DB it created; a downgrade never rewrites the schema"
        );
    }
    if final_version != SCHEMA_VERSION {
        crate::rivet_bail!(
            crate::error::codes::INTERNAL_STATE_INVARIANT,
            "state(pg): migration incomplete — expected schema v{} but reached v{}",
            SCHEMA_VERSION,
            final_version
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{StateConn, StateStore, connect_pg, open_connection};
    use super::*;

    /// The two ladders must define the SAME versions.
    #[test]
    fn both_migration_ladders_define_the_same_versions() {
        let sqlite: Vec<i64> = MIGRATIONS.iter().map(|(v, _)| *v).collect();
        let pg: Vec<i64> = PG_MIGRATIONS.iter().map(|(v, _)| *v).collect();
        assert_eq!(
            sqlite, pg,
            "the SQLite and PostgreSQL migration ladders disagree; the backend missing a \
             version refuses to start, because SCHEMA_VERSION is the last SQLite entry for both"
        );
        assert_eq!(
            SCHEMA_VERSION,
            *sqlite.last().expect("the ladder is not empty"),
            "SCHEMA_VERSION must be the ladder's last version"
        );
        assert!(
            sqlite.windows(2).all(|w| w[0] < w[1]),
            "migration versions must be strictly ascending: {sqlite:?}"
        );
    }

    #[test]
    fn sqlite_and_postgres_migrations_define_the_same_tables_per_version() {
        use std::collections::{BTreeSet, HashMap};
        let table_names = created_table_names;
        let mut pg: HashMap<i64, BTreeSet<String>> = HashMap::new();
        for &(v, sql) in PG_MIGRATIONS {
            pg.entry(v).or_default().extend(table_names(sql));
        }
        const REBUILT_ON_SQLITE_ONLY: &[(i64, &str)] = &[
            (29, "cdc_snapshot"),
            (30, "export_state"),
            (32, "export_load_spec_run"),
            (34, "keyset_range"),
            (38, "export_load_spec"),
        ];
        for &(v, sql) in MIGRATIONS {
            if let Some(pg_tables) = pg.get(&v) {
                let mut sqlite_tables = table_names(sql);
                for (rv, t) in REBUILT_ON_SQLITE_ONLY {
                    if *rv == v {
                        sqlite_tables.remove(*t);
                    }
                }
                assert_eq!(
                    &sqlite_tables, pg_tables,
                    "migration v{v}: SQLite and Postgres define different tables"
                );
            }
        }
    }

    /// Every PG `ALTER TABLE … ADD COLUMN` must be IDEMPOTENT (`IF NOT EXISTS`).
    #[test]
    fn every_pg_add_column_is_idempotent() {
        for &(v, sql) in PG_MIGRATIONS {
            for line in sql.lines() {
                let l = line.trim();
                if l.starts_with("//") || l.starts_with("--") {
                    continue;
                }
                if l.contains("ADD COLUMN") {
                    assert!(
                        l.contains("ADD COLUMN IF NOT EXISTS"),
                        "PG migration v{v} has a non-idempotent ADD COLUMN — must be \
                         `ADD COLUMN IF NOT EXISTS` so the migration is re-runnable: {l}"
                    );
                }
            }
        }
    }

    #[test]
    fn fresh_db_reaches_latest_version() {
        let s = StateStore::open_in_memory().unwrap();
        let ver = match &s.conn {
            StateConn::Sqlite(c) => get_current_version(c),
            StateConn::Postgres(_) => unreachable!(),
        };
        assert_eq!(ver, SCHEMA_VERSION);
    }

    /// A state DB a NEWER rivet migrated is refused by name, before the generic "migration incomplete".
    #[test]
    fn a_state_db_from_a_newer_rivet_is_refused_by_name() {
        let s = StateStore::open_in_memory().unwrap();
        match &s.conn {
            StateConn::Sqlite(c) => {
                migrate(c).unwrap();
                c.execute(
                    "INSERT INTO schema_version (version) VALUES (?1)",
                    [SCHEMA_VERSION + 1],
                )
                .unwrap();
                let e = migrate(c).unwrap_err();
                assert_eq!(
                    crate::error::classify_exit(&e),
                    5,
                    "a protective refusal exits 5"
                );
                assert_eq!(
                    crate::error::error_code(&e),
                    Some("RIVET_STATE_SCHEMA_NEWER")
                );
                let err = format!("{e:#}");
                assert!(err.contains("newer than this rivet knows"), "{err}");
                assert!(!err.contains("migration incomplete"), "{err}");
                assert_eq!(
                    get_current_version(c),
                    SCHEMA_VERSION + 1,
                    "nothing rewritten"
                );
            }
            StateConn::Postgres(_) => unreachable!(),
        }
    }

    /// v34 keeps every chunk run and keyset range a v33 state holds, with no source recorded, and admits one run and one range set per source.
    #[test]
    fn v34_keeps_in_flight_runs_without_a_source_and_keys_new_ones_by_source() {
        let conn = Connection::open_in_memory().unwrap();
        ensure_schema_version_table(&conn);
        for &(ver, sql) in MIGRATIONS {
            if ver <= 33 {
                conn.execute_batch(&format!(
                    "BEGIN;\n{sql}\nINSERT INTO schema_version (version) VALUES ({ver});\nCOMMIT;"
                ))
                .unwrap();
            }
        }
        conn.execute_batch(
            "INSERT INTO chunk_run (run_id, export_name, plan_hash, status, max_chunk_attempts, created_at, updated_at) \
                VALUES ('r1', 'orders', 'h', 'in_progress', 3, 't', 't');
             INSERT INTO keyset_range (export_name, run_id, range_index, lo, hi, done, updated_at, key_column) \
                VALUES ('orders', 'k1', 0, NULL, '5', 1, 't', 'id'), ('orders', 'k1', 1, '5', NULL, 0, 't', 'id');
             INSERT INTO export_state (export_name, prefix, resume_run_id) VALUES ('orders', 'pg/a', 'k1');",
        )
        .unwrap();
        migrate(&conn).unwrap();
        let text = |sql: &str| -> Vec<String> {
            let mut q = conn.prepare(sql).unwrap();
            q.query_map([], |r| r.get::<_, String>(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            text(
                "SELECT run_id || ':' || status || ':' || COALESCE(source, 'none') FROM chunk_run"
            ),
            vec!["r1:in_progress:none"]
        );
        assert_eq!(
            text(
                "SELECT range_index || ':' || COALESCE(lo, '-') || ':' || COALESCE(hi, '-') || ':' || done \
                 || ':' || key_column || ':' || COALESCE(source, 'none') FROM keyset_range ORDER BY range_index"
            ),
            vec!["0:-:5:1:id:none", "1:5:-:0:id:none"]
        );
        assert_eq!(
            text(
                "SELECT COALESCE(resume_owner, 'none') || COALESCE(source_schema, '') || \
                 COALESCE(population, '') || COALESCE(destination, '') FROM export_state"
            ),
            vec!["none"]
        );
        let run = |id: &str, source: &str| {
            conn.execute(
                "INSERT INTO chunk_run (run_id, export_name, source, plan_hash, status, max_chunk_attempts, created_at, updated_at) \
                 VALUES (?1, 'orders', ?2, 'h', 'in_progress', 3, 't', 't')",
                [id, source],
            )
        };
        run("r2", "pg/a").expect("a source's own run beside the unowned one");
        run("r3", "pg/b").expect("another source's run");
        run("r4", "pg/b").expect_err("one in-progress run per source");
        let range = |source: &str| {
            conn.execute(
                "INSERT INTO keyset_range (export_name, source, run_id, range_index, done, updated_at) \
                 VALUES ('orders', ?1, 'k2', 0, 0, 't')",
                [source],
            )
        };
        range("pg/a").expect("a source's own range 0");
        range("pg/a").expect_err("one range per index and source");
    }

    /// v38 keys the by-name load spec by the source.
    #[test]
    fn v38_keys_the_load_spec_by_source_and_keeps_a_v37_row_as_sourceless() {
        let conn = Connection::open_in_memory().unwrap();
        ensure_schema_version_table(&conn);
        for &(ver, sql) in MIGRATIONS {
            if ver <= 37 {
                conn.execute_batch(&format!(
                    "BEGIN;\n{sql}\nINSERT INTO schema_version (version) VALUES ({ver});\nCOMMIT;"
                ))
                .unwrap();
            }
        }
        conn.execute_batch(
            "INSERT INTO export_load_spec (export_name, unit, primary_key_json, key_origin, origin, captured_at) \
             VALUES ('orders', '', '[\"id\"]', 'init', 'init', 't');",
        )
        .unwrap();
        migrate(&conn).unwrap();
        assert_eq!(get_current_version(&conn), SCHEMA_VERSION);
        conn.execute_batch(
            "INSERT INTO export_load_spec (export_name, unit, source, primary_key_json, origin, captured_at) \
             VALUES ('orders', '', 'mysql://h:3306/a', '[\"code\"]', 'run', 't'), \
                    ('orders', '', 'postgres://h:5432/b', '[\"no\"]', 'run', 't');",
        )
        .unwrap();
        let mut q = conn
            .prepare(
                "SELECT source || '=' || primary_key_json FROM export_load_spec ORDER BY source",
            )
            .unwrap();
        let rows: Vec<String> = q
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            rows,
            vec![
                "=[\"id\"]".to_string(),
                "mysql://h:3306/a=[\"code\"]".to_string(),
                "postgres://h:5432/b=[\"no\"]".to_string()
            ]
        );
    }

    /// v37 adds `run_status.plan_id` on a populated v36 state and on a fresh one.
    #[test]
    fn v37_adds_the_applied_plan_to_run_status_on_a_v36_state_and_on_a_fresh_one() {
        for from_v36 in [true, false] {
            let conn = Connection::open_in_memory().unwrap();
            ensure_schema_version_table(&conn);
            if from_v36 {
                for &(ver, sql) in MIGRATIONS {
                    if ver <= 36 {
                        conn.execute_batch(&format!(
                            "BEGIN;\n{sql}\nINSERT INTO schema_version (version) VALUES ({ver});\nCOMMIT;"
                        ))
                        .unwrap();
                    }
                }
                conn.execute_batch(
                    "INSERT INTO run_status (run_id, export_name, prefix, status, started_at) \
                     VALUES ('r0', 'orders', 'file:///out', 'success', '2026-01-01T00:00:00Z');",
                )
                .unwrap();
            }
            migrate(&conn).unwrap();
            assert_eq!(
                get_current_version(&conn),
                SCHEMA_VERSION,
                "from v36 {from_v36}"
            );
            conn.execute_batch(
                "INSERT INTO run_status (run_id, export_name, prefix, status, started_at, plan_id) \
                 VALUES ('r1', 'orders', 'file:///out', 'success', '2026-01-02T00:00:00Z', 'p1');",
            )
            .unwrap();
            let mut q = conn
                .prepare("SELECT run_id || ':' || COALESCE(plan_id, 'none') FROM run_status ORDER BY run_id")
                .unwrap();
            let rows: Vec<String> = q
                .query_map([], |r| r.get::<_, String>(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            let want: Vec<String> = if from_v36 {
                vec!["r0:none".into(), "r1:p1".into()]
            } else {
                vec!["r1:p1".into()]
            };
            assert_eq!(rows, want, "from v36 {from_v36}");
        }
    }

    /// The Postgres ladder's v37: `run_status.plan_id` lands on a populated v36 schema and on a fresh one.
    #[test]
    fn pg_v37_adds_the_applied_plan_to_run_status_on_a_v36_state_and_on_a_fresh_one() {
        let Ok(url) = std::env::var("RIVET_TEST_STATE_URL") else {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL unset");
        };
        if !url.starts_with("postgres") {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL is not a postgres URL");
        }
        let mut client = connect_pg(&url).expect("connect pg state");
        for (schema, from_v36) in [
            ("rivet_v37_from_v36_test", true),
            ("rivet_v37_fresh_test", false),
        ] {
            client
                .batch_execute(&format!(
                    "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}; \
                     SET search_path TO {schema}; \
                     CREATE TABLE rivet_schema_version (version BIGINT NOT NULL);"
                ))
                .unwrap();
            if from_v36 {
                for &(ver, sql) in PG_MIGRATIONS {
                    if ver <= 36 {
                        client
                            .batch_execute(&format!(
                                "BEGIN; {sql} INSERT INTO rivet_schema_version (version) VALUES ({ver}); COMMIT;"
                            ))
                            .unwrap();
                    }
                }
                client
                    .batch_execute(
                        "INSERT INTO run_status (run_id, export_name, prefix, status, started_at) \
                         VALUES ('r0', 'orders', 'file:///out', 'success', '2026-01-01T00:00:00Z');",
                    )
                    .unwrap();
            }
            migrate_pg(&mut client).unwrap_or_else(|e| panic!("{schema}: {e:#}"));
            let version: i64 = client
                .query_one("SELECT MAX(version) FROM rivet_schema_version", &[])
                .unwrap()
                .get(0);
            assert_eq!(version, SCHEMA_VERSION, "{schema}");
            client
                .batch_execute(
                    "INSERT INTO run_status (run_id, export_name, prefix, status, started_at, plan_id) \
                     VALUES ('r1', 'orders', 'file:///out', 'success', '2026-01-02T00:00:00Z', 'p1');",
                )
                .unwrap();
            let rows: Vec<String> = client
                .query(
                    "SELECT run_id || ':' || COALESCE(plan_id, 'none') FROM run_status ORDER BY run_id",
                    &[],
                )
                .unwrap()
                .iter()
                .map(|r| r.get::<_, String>(0))
                .collect();
            let want: Vec<String> = if from_v36 {
                vec!["r0:none".into(), "r1:p1".into()]
            } else {
                vec!["r1:p1".into()]
            };
            assert_eq!(rows, want, "{schema}");
            client
                .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE;"))
                .unwrap();
        }
    }

    /// v29 rebuilds `cdc_snapshot` with the prefix in its key.
    #[test]
    fn v30_keeps_every_cursor_under_the_empty_prefix_and_keys_by_destination() {
        let conn = Connection::open_in_memory().unwrap();
        ensure_schema_version_table(&conn);
        for &(ver, sql) in MIGRATIONS {
            if ver <= 29 {
                conn.execute_batch(&format!(
                    "BEGIN;\n{sql}\nINSERT INTO schema_version (version) VALUES ({ver});\nCOMMIT;"
                ))
                .unwrap();
            }
        }
        conn.execute(
            "INSERT INTO export_state (export_name, last_cursor_value, resume_run_id, cursor_column) \
             VALUES ('orders', '2026-01-01', 'r1', 'updated_at')",
            [],
        )
        .unwrap();
        migrate(&conn).unwrap();
        let row: (String, String, String, String) = conn
            .query_row(
                "SELECT prefix, last_cursor_value, resume_run_id, cursor_column FROM export_state",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            (
                "".into(),
                "2026-01-01".into(),
                "r1".into(),
                "updated_at".into()
            )
        );
        conn.execute(
            "INSERT INTO export_state (export_name, prefix, last_cursor_value) VALUES ('orders', 'b/out', 'x')",
            [],
        )
        .expect("the same name under another destination is its own row");
    }

    #[test]
    fn v29_keeps_legacy_snapshot_rows_and_keys_baselines_by_prefix() {
        let conn = Connection::open_in_memory().unwrap();
        ensure_schema_version_table(&conn);
        for &(ver, sql) in MIGRATIONS {
            if ver <= 28 {
                conn.execute_batch(&format!(
                    "BEGIN;\n{sql}\nINSERT INTO schema_version (version) VALUES ({ver});\nCOMMIT;"
                ))
                .unwrap();
            }
        }
        conn.execute(
            "INSERT INTO cdc_snapshot (export_name, table_name, run_id, completed_at) \
             VALUES ('users', 'users', 'r1', '2026-01-01T00:00:00Z')",
            [],
        )
        .unwrap();
        migrate(&conn).unwrap();
        let legacy_prefix: String = conn
            .query_row(
                "SELECT prefix FROM cdc_snapshot WHERE run_id = 'r1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(legacy_prefix, "", "a pre-v29 row keeps the empty prefix");
        conn.execute(
            "INSERT INTO cdc_snapshot (export_name, table_name, prefix, run_id, completed_at) \
             VALUES ('users', 'users', 'gs://b/pb/', 'r2', '2026-01-02T00:00:00Z')",
            [],
        )
        .expect("a second prefix for the same export/table is its own row");
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM cdc_snapshot", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2);
        conn.execute("SELECT key_column FROM keyset_range", [])
            .unwrap();
    }

    #[test]
    fn migration_is_idempotent() {
        let s = StateStore::open_in_memory().unwrap();
        match &s.conn {
            StateConn::Sqlite(c) => {
                migrate(c).unwrap();
                migrate(c).unwrap();
                assert_eq!(get_current_version(c), SCHEMA_VERSION);
            }
            StateConn::Postgres(_) => unreachable!(),
        }
    }

    /// Only a version equal to this build's schema skips the migration lock.
    #[test]
    fn already_current_is_true_only_at_this_builds_schema_version() {
        assert!(already_current(SCHEMA_VERSION));
        assert!(!already_current(0));
        assert!(!already_current(SCHEMA_VERSION - 1));
        assert!(!already_current(SCHEMA_VERSION + 1));
    }

    /// Opening an already-current database takes no write lock, so it succeeds while another writer holds one.
    #[test]
    fn migrating_a_current_database_does_not_wait_for_the_write_lock() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        migrate(&open_connection(&db).unwrap()).unwrap();
        let holder = open_connection(&db).unwrap();
        holder.execute_batch("BEGIN IMMEDIATE;").unwrap();
        let opener = open_connection(&db).unwrap();
        opener
            .busy_timeout(std::time::Duration::from_millis(200))
            .unwrap();
        migrate(&opener).expect("an already-current open must not need the write lock");
        holder.execute_batch("ROLLBACK;").unwrap();
    }

    /// Several writers migrating ONE database at once all succeed.
    #[test]
    fn several_writers_migrating_one_database_at_once_all_succeed() {
        const WRITERS: usize = 4;
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        let start = std::sync::Barrier::new(WRITERS);

        let results: Vec<Result<()>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..WRITERS)
                .map(|_| {
                    s.spawn(|| {
                        let conn = open_connection(&db)?;
                        start.wait();
                        migrate(&conn)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("a writer thread"))
                .collect()
        });

        for (i, r) in results.iter().enumerate() {
            assert!(
                r.is_ok(),
                "writer {i} of {WRITERS} failed to migrate a shared database: {:?}",
                r.as_ref().err()
            );
        }
        let conn = open_connection(&db).unwrap();
        assert_eq!(
            get_current_version(&conn),
            SCHEMA_VERSION,
            "the ladder must end at the current version exactly once, however many \
             writers raced to apply it"
        );
    }

    #[test]
    fn legacy_db_gets_upgraded() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE export_state (
                export_name TEXT PRIMARY KEY,
                last_cursor_value TEXT,
                last_run_at TEXT
            );
            CREATE TABLE export_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                export_name TEXT NOT NULL,
                run_at TEXT NOT NULL,
                duration_ms INTEGER NOT NULL,
                total_rows INTEGER NOT NULL,
                status TEXT NOT NULL
            );",
        )
        .unwrap();

        migrate(&conn).unwrap();
        assert_eq!(get_current_version(&conn), SCHEMA_VERSION);

        let has_chunk_run: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='chunk_run'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_chunk_run);
    }

    #[test]
    fn upgrading_from_v12_adds_the_ledger_and_snapshot_tables_and_keeps_data() {
        let conn = Connection::open_in_memory().unwrap();
        ensure_schema_version_table(&conn);
        for &(ver, sql) in MIGRATIONS {
            if ver <= 12 {
                conn.execute_batch(&format!(
                    "BEGIN;\n{sql}\nINSERT INTO schema_version (version) VALUES ({ver});\nCOMMIT;"
                ))
                .unwrap();
            }
        }
        assert_eq!(get_current_version(&conn), 12, "staged at v12");
        conn.execute(
            "INSERT INTO export_state (export_name, last_cursor_value, last_run_at) \
             VALUES ('orders', '42', '2026-01-01T00:00:00Z')",
            [],
        )
        .unwrap();

        migrate(&conn).unwrap();
        assert_eq!(get_current_version(&conn), SCHEMA_VERSION);

        for t in ["load_run", "loaded_source_run", "cdc_snapshot"] {
            let exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name = ?1",
                    [t],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(
                exists,
                "{t} missing after the v12→v{SCHEMA_VERSION} upgrade"
            );
        }
        let cursor: String = conn
            .query_row(
                "SELECT last_cursor_value FROM export_state WHERE export_name = 'orders'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cursor, "42", "pre-upgrade data must survive");
    }

    /// The POSTGRES upgrade path: stage a populated state DB at v18 (before the v19 keyset_range table), then migrate it in place to HEAD and assert v19 lands correctly on the EXISTING db.
    #[test]
    fn pg_upgrade_from_v18_lands_keyset_range_as_bigint_and_keeps_data() {
        let Ok(url) = std::env::var("RIVET_TEST_STATE_URL") else {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL unset");
        };
        if !url.starts_with("postgres") {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL is not a postgres URL");
        }
        let mut client = connect_pg(&url).expect("connect pg state");
        client
            .batch_execute(
                "DROP SCHEMA IF EXISTS rivet_upgrade_test CASCADE; \
                 DROP SCHEMA IF EXISTS rivet_state_backup_v18 CASCADE; \
                 CREATE SCHEMA rivet_upgrade_test; SET search_path TO rivet_upgrade_test;",
            )
            .unwrap();

        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS rivet_schema_version (version BIGINT NOT NULL);",
            )
            .unwrap();
        for &(ver, sql) in PG_MIGRATIONS {
            if ver <= 18 {
                client
                    .batch_execute(&format!(
                        "BEGIN; {sql} INSERT INTO rivet_schema_version (version) VALUES ({ver}); COMMIT;"
                    ))
                    .unwrap();
            }
        }
        client
            .batch_execute(
                "INSERT INTO export_state (export_name, last_cursor_value, last_run_at) \
                 VALUES ('orders', '42', '2026-01-01T00:00:00Z')",
            )
            .unwrap();

        migrate_pg(&mut client).expect("v18 -> HEAD upgrade must apply cleanly on a populated db");

        let dtype: String = client
            .query_one(
                "SELECT data_type FROM information_schema.columns \
                 WHERE table_schema = 'rivet_upgrade_test' AND table_name = 'keyset_range' \
                   AND column_name = 'range_index'",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            dtype, "bigint",
            "v19 keyset_range.range_index must upgrade to BIGINT, not int4 (bug #1)"
        );
        let cursor: String = client
            .query_one(
                "SELECT last_cursor_value FROM export_state WHERE export_name = 'orders'",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            cursor, "42",
            "pre-upgrade cursor must survive the migration"
        );

        client
            .batch_execute(
                "DROP SCHEMA IF EXISTS rivet_upgrade_test CASCADE; \
                 DROP SCHEMA IF EXISTS rivet_state_backup_v18 CASCADE;",
            )
            .unwrap();
    }

    #[test]
    fn pg_v32_replays_over_a_load_spec_table_already_in_its_v32_shape() {
        let Ok(url) = std::env::var("RIVET_TEST_STATE_URL") else {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL unset");
        };
        if !url.starts_with("postgres") {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL is not a postgres URL");
        }
        let mut client = connect_pg(&url).expect("connect pg state");
        client
            .batch_execute(
                "DROP SCHEMA IF EXISTS rivet_v32_replay_test CASCADE; \
                 DROP SCHEMA IF EXISTS rivet_state_backup_v31 CASCADE; \
                 CREATE SCHEMA rivet_v32_replay_test; SET search_path TO rivet_v32_replay_test; \
                 CREATE TABLE rivet_schema_version (version BIGINT NOT NULL);",
            )
            .unwrap();
        for &(ver, sql) in PG_MIGRATIONS {
            if ver <= 31 {
                client
                    .batch_execute(&format!(
                        "BEGIN; {sql} INSERT INTO rivet_schema_version (version) VALUES ({ver}); COMMIT;"
                    ))
                    .unwrap();
            }
        }
        client
            .batch_execute(
                "INSERT INTO export_load_spec_run \
                     (export_name, unit, run_id, columns_json, primary_key_json, captured_at) \
                 VALUES ('t', '', 'r1', '[a]', NULL, '1'), ('t', '', 'r2', '[a]', NULL, '2');",
            )
            .unwrap();
        migrate_pg(&mut client).expect("v31 -> v32 on a populated db");
        let others: Vec<String> = client
            .query(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'rivet_v32_replay_test' \
                   AND tablename NOT IN ('export_load_spec_run', 'load_spec_version')",
                &[],
            )
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        client
            .batch_execute(&format!("DROP TABLE {} CASCADE;", others.join(", ")))
            .unwrap();
        migrate_pg(&mut client).expect("the ladder must replay over a v32-shaped load-spec table");
        let unresolved: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM export_load_spec_run r \
                 LEFT JOIN load_spec_version v USING (spec_id) WHERE v.spec_id IS NULL",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            unresolved, 0,
            "every run row still resolves to its spec version"
        );
        client
            .batch_execute(
                "DROP SCHEMA IF EXISTS rivet_v32_replay_test CASCADE; \
                 DROP SCHEMA IF EXISTS rivet_state_backup_v31 CASCADE;",
            )
            .unwrap();
    }

    #[test]
    fn v8_renames_file_manifest_to_file_log() {
        let s = StateStore::open_in_memory().unwrap();
        let conn = match &s.conn {
            StateConn::Sqlite(c) => c,
            StateConn::Postgres(_) => unreachable!(),
        };
        let has_file_log: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='file_log'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_file_log, "v8 must produce a `file_log` table");
        let has_old: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='file_manifest'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!has_old, "v8 must remove the old `file_manifest` table");
        let has_new_idx: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='index' AND name='idx_file_log_export'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_new_idx, "v8 must create the renamed index");
    }

    #[test]
    fn v8_upgrades_existing_v7_db_with_data() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        conn.execute(
            "INSERT INTO file_log (run_id, export_name, file_name, row_count, bytes, format, created_at)
             VALUES ('r1', 'orders', 'f.parquet', 100, 4096, 'parquet', '2026-05-21T00:00:00Z')",
            [],
        )
        .unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM file_log", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn run_aggregate_table_exists_after_migration() {
        let s = StateStore::open_in_memory().unwrap();
        let conn = match &s.conn {
            StateConn::Sqlite(c) => c,
            StateConn::Postgres(_) => unreachable!(),
        };
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='run_aggregate'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(exists, "v5 migration must create the run_aggregate table");
    }

    #[test]
    fn v13_creates_the_load_ledger_tables() {
        let s = StateStore::open_in_memory().unwrap();
        let conn = match &s.conn {
            StateConn::Sqlite(c) => c,
            StateConn::Postgres(_) => unreachable!(),
        };
        for table in ["load_run", "loaded_source_run"] {
            let exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "v13 migration must create `{table}`");
        }
    }

    #[test]
    fn v14_creates_the_cdc_snapshot_table() {
        let s = StateStore::open_in_memory().unwrap();
        let conn = match &s.conn {
            StateConn::Sqlite(c) => c,
            StateConn::Postgres(_) => unreachable!(),
        };
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='cdc_snapshot'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(exists, "v14 migration must create the cdc_snapshot table");
    }

    /// A SQLite state file staged at exactly schema v`upto`, with one incremental cursor row.
    fn stage_sqlite(path: &std::path::Path, upto: i64) -> Connection {
        let conn = open_connection(path).unwrap();
        ensure_schema_version_table(&conn);
        for &(ver, sql) in MIGRATIONS {
            if ver <= upto {
                conn.execute_batch(&format!(
                    "BEGIN;\n{sql}\nINSERT INTO schema_version (version) VALUES ({ver});\nCOMMIT;"
                ))
                .unwrap();
            }
        }
        conn.execute_batch(
            "INSERT INTO export_state (export_name, prefix, last_cursor_value) \
             VALUES ('orders', 'local/a', '42');",
        )
        .unwrap();
        conn
    }

    /// The `.bak` copies beside a state file, by name.
    fn copies_beside(db: &std::path::Path) -> Vec<String> {
        let name = db.file_name().unwrap().to_str().unwrap().to_string();
        let mut out: Vec<String> = std::fs::read_dir(db.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_str().unwrap().to_string())
            .filter(|f| f.starts_with(&name) && f.ends_with(".bak"))
            .collect();
        out.sort();
        out
    }

    fn version_of(path: &std::path::Path) -> i64 {
        let c = Connection::open(path).unwrap();
        get_current_version(&c)
    }

    fn has_column(c: &Connection, table: &str, column: &str) -> bool {
        c.query_row(
            &format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?1"),
            [column],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    }

    /// A copy is written for a state behind this build only.
    #[test]
    fn the_copy_is_written_only_for_a_state_behind_this_build() {
        assert!(!needs_copy(0), "a new state has nothing to go back to");
        assert!(needs_copy(1));
        assert!(needs_copy(32), "0.31.0's v32");
        assert!(needs_copy(SCHEMA_VERSION - 1));
        assert!(!needs_copy(SCHEMA_VERSION), "nothing to migrate");
        assert!(
            !needs_copy(SCHEMA_VERSION + 1),
            "a newer state is refused, never rewritten"
        );
    }

    /// The ladder parser names every table the copy holds, including the one the ladder renamed to.
    #[test]
    fn the_ladder_parser_names_the_tables_the_copy_holds() {
        let names = created_table_names(
            "CREATE TABLE a (x INT); create table if not exists b_2 (y TEXT);\n\
             ALTER TABLE a RENAME TO c; CREATE INDEX i ON a(x);",
        );
        assert_eq!(
            names,
            std::collections::BTreeSet::from(["a".to_string(), "b_2".to_string()])
        );
        let pg = pg_state_table_names();
        for t in [
            "rivet_schema_version",
            "export_state",
            "export_metrics",
            "file_manifest",
            "file_log",
            "keyset_range",
            "load_spec_version",
            "state_lease",
            "run_status",
        ] {
            assert!(pg.iter().any(|n| n == t), "{t} missing from {pg:?}");
        }
        assert!(pg.len() >= 20, "{pg:?}");
        assert_eq!(pg_backup_schema(32), "rivet_state_backup_v32");
    }

    /// The copy is the state as the previous release left it.
    #[test]
    fn a_migration_copies_the_old_state_beside_it_before_the_first_ddl() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join(".rivet_state.db");
        let conn = stage_sqlite(&db, 32);
        assert_eq!(
            copies_beside(&db),
            Vec::<String>::new(),
            "no copy before the migration"
        );

        migrate(&conn).unwrap();

        assert_eq!(get_current_version(&conn), SCHEMA_VERSION);
        let copies = copies_beside(&db);
        assert_eq!(copies.len(), 1, "exactly one copy: {copies:?}");
        let re = regex::Regex::new(r"^\.rivet_state\.db\.v32\.\d{8}T\d{6}Z\.bak$").unwrap();
        assert!(
            re.is_match(&copies[0]),
            "the copy names the version and the UTC time: {}",
            copies[0]
        );
        let copy = Connection::open(dir.path().join(&copies[0])).unwrap();
        assert_eq!(get_current_version(&copy), 32, "the copy is the v32 state");
        assert!(
            !has_column(&copy, "export_state", "stream"),
            "the copy predates the first DDL (v33 adds export_state.stream)"
        );
        assert!(
            has_column(&conn, "export_state", "stream"),
            "the state itself was migrated"
        );
        let cursor: String = copy
            .query_row(
                "SELECT last_cursor_value FROM export_state WHERE export_name = 'orders'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cursor, "42", "the copy holds the previous release's cursor");
    }

    /// A state that is new (version 0) or already current is migrated or opened with no copy.
    #[test]
    fn a_new_or_current_or_newer_state_gets_no_copy() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join(".rivet_state.db");
        let conn = open_connection(&db).unwrap();
        migrate(&conn).unwrap();
        assert_eq!(
            copies_beside(&db),
            Vec::<String>::new(),
            "a new state: nothing to go back to"
        );
        migrate(&conn).unwrap();
        assert_eq!(
            copies_beside(&db),
            Vec::<String>::new(),
            "an already-current state: no migration"
        );
        conn.execute(
            "INSERT INTO schema_version (version) VALUES (?1)",
            [SCHEMA_VERSION + 1],
        )
        .unwrap();
        let e = migrate(&conn).unwrap_err();
        assert_eq!(
            crate::error::error_code(&e),
            Some("RIVET_STATE_SCHEMA_NEWER")
        );
        assert_eq!(
            copies_beside(&db),
            Vec::<String>::new(),
            "a newer state: nothing is rewritten, nothing copied"
        );
    }

    /// Puts the permission bits back when dropped, so the temp dir can be removed.
    #[cfg(unix)]
    struct Mode(std::path::PathBuf, u32);
    #[cfg(unix)]
    impl Drop for Mode {
        fn drop(&mut self) {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(self.1));
        }
    }

    /// A copy that cannot be written refuses the migration by code, and the state file is byte-identical afterwards.
    #[cfg(unix)]
    #[test]
    fn a_copy_that_cannot_be_written_refuses_the_migration_and_leaves_the_state_as_it_was() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join(".rivet_state.db");
        let conn = stage_sqlite(&db, 32);
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .unwrap();
        let before = std::fs::read(&db).unwrap();
        for suffix in ["-wal", "-shm"] {
            let p = dir.path().join(format!(".rivet_state.db{suffix}"));
            if !p.exists() {
                std::fs::write(&p, b"").unwrap();
            }
        }
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
        let _restore = Mode(dir.path().to_path_buf(), 0o755);
        if std::fs::write(dir.path().join("probe"), b"").is_ok() {
            return crate::test_hook::skip_live("a read-only directory is writable for this user");
        }

        let e = migrate(&conn).unwrap_err();

        crate::error::assert_coded(&e, "RIVET_STATE_BACKUP_FAILED", 1);
        let said = format!("{e:#}");
        assert!(
            said.contains(".rivet_state.db.v32."),
            "names the copy it could not write: {said}"
        );
        assert!(said.contains("Nothing was migrated"), "{said}");
        assert_eq!(get_current_version(&conn), 32, "the state is still v32");
        assert!(!has_column(&conn, "export_state", "stream"), "no DDL ran");
        assert_eq!(
            copies_beside(&db),
            Vec::<String>::new(),
            "no half-written copy is left behind"
        );
        drop(conn);
        assert_eq!(
            std::fs::read(&db).unwrap(),
            before,
            "the state file is byte-identical"
        );
        assert_eq!(version_of(&db), 32);
    }

    /// The PostgreSQL copy holds every state table with the old version's rows, keys and its own serial sequences.
    #[test]
    fn pg_a_migration_copies_every_state_table_into_a_backup_schema_before_the_first_ddl() {
        let Ok(url) = std::env::var("RIVET_TEST_STATE_URL") else {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL unset");
        };
        if !url.starts_with("postgres") {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL is not a postgres URL");
        }
        let mut client = connect_pg(&url).expect("connect pg state");
        let reset = "DROP SCHEMA IF EXISTS rivet_wayback_test CASCADE; \
                     DROP SCHEMA IF EXISTS rivet_state_backup_v32 CASCADE;";
        let stage = |client: &mut postgres::Client, metrics_rows: i64| {
            client
                .batch_execute(
                    "CREATE SCHEMA rivet_wayback_test; SET search_path TO rivet_wayback_test; \
                     CREATE TABLE rivet_schema_version (version BIGINT NOT NULL);",
                )
                .unwrap();
            for &(ver, sql) in PG_MIGRATIONS {
                if ver <= 32 {
                    client
                        .batch_execute(&format!(
                            "BEGIN; {sql} INSERT INTO rivet_schema_version (version) VALUES ({ver}); COMMIT;"
                        ))
                        .unwrap();
                }
            }
            client
                .batch_execute(&format!(
                    "INSERT INTO export_state (export_name, prefix, last_cursor_value) VALUES ('orders', 'pg/a', '42'); \
                     INSERT INTO export_metrics (export_name, run_at, duration_ms, total_rows, status) \
                         SELECT 'orders', 't', 1, g, 'ok' FROM generate_series(1, {metrics_rows}) g;"
                ))
                .unwrap();
        };
        client.batch_execute(reset).unwrap();
        stage(&mut client, 3);
        let tables = |client: &mut postgres::Client, schema: &str| -> Vec<(String, i64)> {
            let names: Vec<String> = client
                .query(
                    "SELECT tablename::text FROM pg_tables WHERE schemaname = $1 ORDER BY 1",
                    &[&schema],
                )
                .unwrap()
                .iter()
                .map(|r| r.get(0))
                .collect();
            names
                .into_iter()
                .map(|t| {
                    let n: i64 = client
                        .query_one(&format!("SELECT COUNT(*) FROM {schema}.{t}"), &[])
                        .unwrap()
                        .get(0);
                    (t, n)
                })
                .collect()
        };
        let before = tables(&mut client, "rivet_wayback_test");
        assert!(before.iter().any(|(t, n)| t == "export_metrics" && *n == 3));

        migrate_pg(&mut client).expect("v32 -> HEAD on a populated db");

        let copy = tables(&mut client, "rivet_state_backup_v32");
        assert_eq!(
            copy, before,
            "the copy holds every table with the old version's row counts"
        );
        let ver: i64 = client
            .query_one(
                "SELECT MAX(version) FROM rivet_state_backup_v32.rivet_schema_version",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(ver, 32, "the copy is the v32 state");
        let stream_cols: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'rivet_state_backup_v32' \
                 AND table_name = 'export_state' AND column_name = 'stream'",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            stream_cols, 0,
            "the copy predates the first DDL (v33 adds export_state.stream)"
        );
        let pkeys: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid \
                 WHERE c.relnamespace = 'rivet_state_backup_v32'::regnamespace AND i.indisunique",
                &[],
            )
            .unwrap()
            .get(0);
        assert!(
            pkeys >= 5,
            "the copy keeps the keys the previous release's upserts need: {pkeys}"
        );
        let seq: String = client
            .query_one(
                "SELECT pg_get_serial_sequence('rivet_state_backup_v32.export_metrics', 'id')",
                &[],
            )
            .unwrap()
            .get(0);
        assert!(
            seq.starts_with("rivet_state_backup_v32."),
            "the copy's serial column has its own sequence: {seq}"
        );
        let next: i64 = client
            .query_one(
                "INSERT INTO rivet_state_backup_v32.export_metrics (export_name, run_at, duration_ms, total_rows, status) \
                 VALUES ('orders', 't', 1, 0, 'ok') RETURNING id",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            next, 4,
            "the copy's sequence continues where the state's was"
        );

        client
            .batch_execute("DROP SCHEMA rivet_wayback_test CASCADE;")
            .unwrap();
        stage(&mut client, 7);
        migrate_pg(&mut client).expect("a second v32 -> HEAD beside an existing copy");
        let kept: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM rivet_state_backup_v32.export_metrics",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            kept, 4,
            "the earlier copy is kept, not replaced by the 7-row state"
        );

        client.batch_execute(reset).unwrap();
    }

    /// A role that cannot CREATE on the database cannot write the copy.
    #[test]
    fn pg_a_copy_that_cannot_be_written_refuses_the_migration_and_leaves_the_state_as_it_was() {
        let Ok(url) = std::env::var("RIVET_TEST_STATE_URL") else {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL unset");
        };
        if !url.starts_with("postgres") {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL is not a postgres URL");
        }
        let mut admin = connect_pg(&url).expect("connect pg state");
        let superuser: bool = admin
            .query_one(
                "SELECT rolsuper FROM pg_roles WHERE rolname = current_user",
                &[],
            )
            .unwrap()
            .get(0);
        if !superuser {
            return crate::test_hook::skip_live(
                "RIVET_TEST_STATE_URL is not a superuser: cannot create the test role",
            );
        }
        let role_name = format!("rivet_wayback_nocreate_{}", std::process::id());
        let reset = format!(
            "DROP SCHEMA IF EXISTS {role_name} CASCADE; \
             DROP SCHEMA IF EXISTS rivet_state_backup_v30 CASCADE; \
             DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role_name}') \
             THEN EXECUTE 'DROP OWNED BY {role_name}'; END IF; END $$; \
             DROP ROLE IF EXISTS {role_name};"
        );
        admin.batch_execute(&reset).unwrap();
        admin
            .batch_execute(&format!(
                "CREATE SCHEMA {role_name}; SET search_path TO {role_name}; \
                 CREATE TABLE rivet_schema_version (version BIGINT NOT NULL);"
            ))
            .unwrap();
        for &(ver, sql) in PG_MIGRATIONS {
            if ver <= 30 {
                admin
                    .batch_execute(&format!(
                        "BEGIN; {sql} INSERT INTO rivet_schema_version (version) VALUES ({ver}); COMMIT;"
                    ))
                    .unwrap();
            }
        }
        admin
            .batch_execute(&format!(
                "INSERT INTO export_state (export_name, prefix, last_cursor_value) VALUES ('orders', 'pg/a', '42'); \
                 CREATE ROLE {role_name} LOGIN PASSWORD 'nocreate'; \
                 GRANT USAGE, CREATE ON SCHEMA {role_name} TO {role_name}; \
                 GRANT ALL ON ALL TABLES IN SCHEMA {role_name} TO {role_name}; \
                 GRANT ALL ON ALL SEQUENCES IN SCHEMA {role_name} TO {role_name};"
            ))
            .unwrap();
        let host_db = url
            .rsplit_once('@')
            .map_or(url.as_str(), |(_, rest)| rest)
            .split('?')
            .next()
            .unwrap();
        let role_url = format!(
            "postgresql://{role_name}:nocreate@{host_db}?options=-c%20search_path%3D{role_name}"
        );
        let mut role = connect_pg(&role_url).expect("connect as the role without CREATE");

        let e = migrate_pg(&mut role).unwrap_err();

        crate::error::assert_coded(&e, "RIVET_STATE_BACKUP_FAILED", 1);
        let said = format!("{e:#}");
        assert!(
            said.contains("rivet_state_backup_v30"),
            "names the schema it could not write: {said}"
        );
        assert!(said.contains("Nothing was migrated"), "{said}");
        let ver: i64 = admin
            .query_one(
                &format!("SELECT MAX(version) FROM {role_name}.rivet_schema_version"),
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(ver, 30, "the state is still v30");
        let stream_cols: i64 = admin
            .query_one(
                &format!(
                    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '{role_name}' \
                     AND table_name = 'export_state' AND column_name = 'stream'"
                ),
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(stream_cols, 0, "no DDL ran");
        let copies: i64 = admin
            .query_one(
                "SELECT COUNT(*) FROM pg_namespace WHERE nspname = 'rivet_state_backup_v30'",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(copies, 0, "no half-written copy is left behind");
        drop(role);
        admin.batch_execute(&reset).unwrap();
    }

    /// The lease file `token` beside the state file `db`, locked as a live rivet holds it; released when dropped.
    fn hold_lease_beside(db: &std::path::Path, token: &str) -> std::fs::File {
        use std::os::unix::io::AsRawFd;
        let mut name = db.file_name().unwrap().to_os_string();
        name.push(format!(".lease-{token}"));
        let file = std::fs::File::create(db.with_file_name(name)).unwrap();
        assert_eq!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        file
    }

    /// A state behind this build is not migrated while another open file holds a lease beside it.
    #[test]
    fn a_state_behind_this_build_is_not_migrated_while_a_lease_beside_it_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join(".rivet_state.db");
        let old = SCHEMA_VERSION - 1;
        let conn = stage_sqlite(&db, old);
        let held = hold_lease_beside(&db, "chunk-run-orders");

        let e = migrate(&conn).unwrap_err();

        crate::error::assert_coded(&e, "RIVET_STATE_UPGRADE_BESIDE_LIVE_RUN", 2);
        assert_eq!(
            e.to_string(),
            format!(
                "state: this state is at schema v{old} and its lease \
                 `.rivet_state.db.lease-chunk-run-orders` (the run or load of the export or table \
                 it names) is held by another rivet process on this host, a live rivet whose \
                 release the lease does not record: migrating the state to v{SCHEMA_VERSION} now \
                 would change its schema under that run, and an older release cannot open a \
                 v{SCHEMA_VERSION} state. Nothing was migrated and nothing was written. Stop or \
                 wait for that run of the older release, then run again, or give `--lock-wait` the \
                 seconds it needs."
            )
        );
        assert_eq!(get_current_version(&conn), old, "nothing was migrated");
        assert_eq!(
            copies_beside(&db),
            Vec::<String>::new(),
            "nothing was copied"
        );

        drop(held);
        migrate(&conn).unwrap();
        assert_eq!(
            get_current_version(&conn),
            SCHEMA_VERSION,
            "free, it migrates"
        );
        let _held = hold_lease_beside(&db, "chunk-run-orders");
        migrate(&conn).unwrap();
        assert_eq!(get_current_version(&conn), SCHEMA_VERSION);
    }

    /// The wait asks again until the lease is free, and asks once when there is no time to wait.
    #[test]
    fn a_migration_waits_for_a_held_lease_only_as_long_as_it_may() {
        let lease = || Some(("l".to_string(), "h".to_string()));
        let mut asked = 0;
        wait_while_a_lease_is_held(std::time::Duration::from_secs(30), || {
            asked += 1;
            Ok(if asked < 2 { lease() } else { None })
        })
        .unwrap();
        assert_eq!(asked, 2, "asked until the lease was free");
        let mut asked = 0;
        let started = std::time::Instant::now();
        wait_while_a_lease_is_held(std::time::Duration::ZERO, || {
            asked += 1;
            Ok(lease())
        })
        .unwrap();
        assert_eq!(asked, 1, "no wait: asked once");
        assert!(started.elapsed() < crate::test_budget::ceiling(std::time::Duration::from_secs(1)));
        let mut asked = 0;
        let started = std::time::Instant::now();
        wait_while_a_lease_is_held(std::time::Duration::from_millis(1200), || {
            asked += 1;
            Ok(lease())
        })
        .unwrap();
        let took = started.elapsed();
        assert!(
            took >= std::time::Duration::from_millis(1150)
                && took < crate::test_budget::ceiling(std::time::Duration::from_millis(1800)),
            "held throughout: waited {took:?} for a 1.2 s wait"
        );
        assert_eq!(asked, 3, "asked at 0 s, 1 s and 1.2 s");
        let e = wait_while_a_lease_is_held(std::time::Duration::from_secs(30), || {
            Err(anyhow::anyhow!("unreadable"))
        })
        .unwrap_err();
        assert_eq!(e.to_string(), "unreadable");
    }

    /// Lease files nobody holds (runs that ended) and files of another state in the directory do not stop a migration.
    #[test]
    fn a_lease_file_nobody_holds_or_of_another_state_does_not_stop_a_migration() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join(".rivet_state.db");
        let conn = stage_sqlite(&db, 32);
        drop(hold_lease_beside(&db, "p.d.orders-00000000000000ff"));
        let _other = hold_lease_beside(&dir.path().join("other.db"), "orders");

        migrate(&conn).unwrap();

        assert_eq!(get_current_version(&conn), SCHEMA_VERSION);
        assert_eq!(copies_beside(&db).len(), 1, "the copy is written as before");
    }

    /// On a PostgreSQL state behind this build a lease row of a live holder stops the migration by code, naming the holder, with nothing migrated or copied.
    #[test]
    fn pg_a_state_behind_this_build_is_not_migrated_while_a_live_holder_keeps_a_lease_row() {
        let Ok(url) = std::env::var("RIVET_TEST_STATE_URL") else {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL unset");
        };
        if !url.starts_with("postgres") {
            return crate::test_hook::skip_live("RIVET_TEST_STATE_URL is not a postgres URL");
        }
        let schema = format!("rivet_lease_mig_test_{}", std::process::id());
        let old = SCHEMA_VERSION - 1;
        let mut client = connect_pg(&url).expect("connect pg state");
        let reset = format!("DROP SCHEMA IF EXISTS {schema} CASCADE;");
        client.batch_execute(&reset).unwrap();
        client
            .batch_execute(&format!(
                "CREATE SCHEMA {schema}; SET search_path TO {schema}; \
                 CREATE TABLE rivet_schema_version (version BIGINT NOT NULL);"
            ))
            .unwrap();
        for &(ver, sql) in PG_MIGRATIONS {
            if ver <= old {
                client
                    .batch_execute(&format!(
                        "BEGIN; {sql} INSERT INTO rivet_schema_version (version) VALUES ({ver}); COMMIT;"
                    ))
                    .unwrap();
            }
        }
        let mut dead = std::process::Command::new("true").spawn().unwrap();
        let dead_pid = dead.id();
        dead.wait().unwrap();
        let here = super::super::load_lease::host_name();
        client
            .execute(
                "INSERT INTO state_lease (lease_key, holder, expires_at) VALUES \
                 ('a.expired', 'elsewhere:7:1', now() - interval '1 second'), \
                 ('b.dead_here', $1, now() + interval '60 seconds'), \
                 ('c.live', 'elsewhere:4242:1', now() + interval '60 seconds')",
                &[&format!("{here}:{dead_pid}:1")],
            )
            .unwrap();
        let version = |c: &mut postgres::Client| -> i64 {
            c.query_one("SELECT MAX(version) FROM rivet_schema_version", &[])
                .unwrap()
                .get(0)
        };

        let copies = |c: &mut postgres::Client| -> i64 {
            c.query_one(
                "SELECT COUNT(*) FROM pg_namespace WHERE nspname = $1",
                &[&pg_backup_schema(old)],
            )
            .unwrap()
            .get(0)
        };
        let copies_before = copies(&mut client);

        let e = migrate_pg(&mut client).unwrap_err();

        crate::error::assert_coded(&e, "RIVET_STATE_UPGRADE_BESIDE_LIVE_RUN", 2);
        assert!(
            e.to_string().starts_with(&format!(
                "state: this state is at schema v{old} and its lease `c.live` (the run or load of \
                 the export or table it names) is held by pid 4242 on host elsewhere, a live rivet \
                 whose release the lease does not record: migrating the state to v{SCHEMA_VERSION} \
                 now"
            )),
            "{e}"
        );
        assert_eq!(version(&mut client), old, "nothing was migrated");
        assert_eq!(copies(&mut client), copies_before, "nothing was copied");

        client
            .execute("DELETE FROM state_lease WHERE lease_key = 'c.live'", &[])
            .unwrap();
        let mut tx = client.transaction().unwrap();
        assert_eq!(
            super::super::load_lease::pg_lease_held(&mut tx).unwrap(),
            None,
            "an expired row and a dead holder on this host are not live"
        );
        drop(tx);
        client.batch_execute(&reset).unwrap();
    }
}

//! `source.columns_from: grants` is accepted on every engine, in every mode, read back through `Config::from_yaml`.

use super::*;

const ENGINES: [(&str, &str); 5] = [
    ("postgres", "postgresql://u:p@localhost/test"),
    ("mysql", "mysql://u:p@localhost/test"),
    ("mssql", "sqlserver://u:p@localhost:1433/test"),
    ("oracle", "oracle://u:p@localhost:1521/test"),
    ("mongo", "mongodb://u:p@localhost/test"),
];

fn config(engine: &str, url: &str, columns_from: bool, export: &str) -> String {
    let policy = if columns_from {
        "  columns_from: grants\n"
    } else {
        ""
    };
    format!(
        "source:\n  type: {engine}\n  url: \"{url}\"\n{policy}exports:\n  - name: t\n    format: parquet\n    destination:\n      type: local\n      path: ./out\n{export}"
    )
}

const BATCH: &str = "    table: orders\n    mode: full\n";
const CDC: &str = "    table: orders\n    mode: cdc\n    cdc: { slot: s1, server_id: 5000, checkpoint: /tmp/c.ckpt, capture_instance: dbo_orders }\n";

#[test]
fn columns_from_grants_loads_on_every_engine_in_every_mode() {
    for (engine, url) in ENGINES {
        for (shape, export) in [("batch", BATCH), ("cdc", CDC)] {
            let text = config(engine, url, true, export);
            let cfg = Config::from_yaml(&text)
                .unwrap_or_else(|e| panic!("{engine} {shape} must load: {e:#}"));
            assert!(cfg.source.columns_from_grants(), "{engine} {shape}");
        }
    }
}

#[test]
fn every_engine_without_columns_from_loads_as_before() {
    for (engine, url) in ENGINES {
        for (shape, export) in [("batch", BATCH), ("cdc", CDC)] {
            let text = config(engine, url, false, export);
            Config::from_yaml(&text)
                .unwrap_or_else(|e| panic!("{engine} {shape} without the policy must load: {e:#}"));
        }
    }
}

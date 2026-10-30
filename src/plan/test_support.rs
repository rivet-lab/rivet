//! The one `ResolvedRunPlan` every test module builds from; a test overrides only what it asserts on.

use super::{ExtractionStrategy, ResolvedRunPlan};
use crate::config::{
    CompressionType, DestinationConfig, DestinationType, FormatType, SourceConfig, SourceType,
};
use crate::tuning::SourceTuning;

/// A snapshot plan to a local path, built from a source URL that is never dialled.
pub(crate) fn test_plan() -> ResolvedRunPlan {
    ResolvedRunPlan {
        split_window: None,
        bytes_read: Default::default(),
        parts_landed: Default::default(),
        single_pass_of: None,
        export_name: "orders".into(),
        partition_rollover: None,
        source_table: None,
        base_query: "SELECT 1".into(),
        query_template: None,
        catalog_query: None,
        destination_written: None,
        is_split_unit: false,
        strategy: ExtractionStrategy::Snapshot,
        format: FormatType::Parquet,
        compression: CompressionType::None,
        compression_level: None,
        max_file_size_bytes: None,
        skip_empty: false,
        meta_columns: Default::default(),
        destination: DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some("/tmp".into()),
            ..Default::default()
        },
        quality: None,
        tuning: SourceTuning::from_config(None),
        tuning_profile_label: "balanced".into(),
        validate: false,
        reconcile: false,
        resume: false,
        source: SourceConfig {
            source_type: SourceType::Postgres,
            url: Some("postgresql://nobody@127.0.0.1:9999/nonexistent".into()),
            url_env: None,
            url_file: None,
            url_secret: None,
            host: None,
            port: None,
            user: None,
            password: None,
            password_env: None,
            password_secret: None,
            database: None,
            environment: None,
            tuning: None,
            tls: None,
            mongo: None,
            columns_from: None,
        },
        column_overrides: Default::default(),
        verify: crate::config::VerifyMode::Size,
        schema_drift_policy: Default::default(),
        shape_drift_warn_factor: 0.0,
        parquet: None,
    }
}

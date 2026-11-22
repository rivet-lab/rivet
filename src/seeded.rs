//! Seeded-defect switches for the recall gate: one build holds every defect, `RIVET_SEED` names the active one.

#[cfg(feature = "seeded")]
pub(crate) fn on(name: &str) -> bool {
    static ACTIVE: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    ACTIVE
        .get_or_init(|| {
            std::env::var("RIVET_SEED")
                .map(|v| v.split(',').map(str::to_owned).collect())
                .unwrap_or_default()
        })
        .iter()
        .any(|n| n == name)
}

/// `seeded!(name, defect, good)` is `good` unless the `seeded` feature is on and `RIVET_SEED` names `name`.
#[cfg(feature = "seeded")]
macro_rules! seeded {
    ($name:literal, $bad:expr, $good:expr) => {
        if $crate::seeded::on($name) {
            $bad
        } else {
            $good
        }
    };
}

#[cfg(not(feature = "seeded"))]
macro_rules! seeded {
    ($name:literal, $bad:expr, $good:expr) => {
        ::std::convert::identity($good)
    };
}

/// The seeded defect: the unscaled value as an f64 would carry it, refused past 2^53 as the driver's f64 money was.
#[cfg(feature = "seeded")]
pub(crate) fn money_through_f64(value: i128, scale: u8) -> anyhow::Result<i128> {
    if (value as f64).abs() >= 9_007_199_254_740_992.0 {
        crate::rivet_bail!(
            crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE,
            "seeded: a money value past 2^53 scaled units was rounded by an f64 (scale {scale})"
        );
    }
    Ok(value)
}

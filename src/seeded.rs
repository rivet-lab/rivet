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
        $good
    };
}

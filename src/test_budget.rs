//! Wall-clock ceilings of unit tests, widened when the binary runs under coverage instrumentation.

use std::time::Duration;

const INSTRUMENTED_FACTOR: u32 = 8;

/// `ceiling` widened by the instrumentation factor when `instrumented`.
fn scaled(ceiling: Duration, instrumented: bool) -> Duration {
    if instrumented {
        ceiling * INSTRUMENTED_FACTOR
    } else {
        ceiling
    }
}

/// An upper wall-clock bound for a test: as written, or widened under `cargo llvm-cov`.
pub(crate) fn ceiling(ceiling: Duration) -> Duration {
    scaled(ceiling, std::env::var_os("CARGO_LLVM_COV").is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ceiling_is_widened_only_under_instrumentation() {
        let five = Duration::from_secs(5);
        assert_eq!(scaled(five, false), five);
        assert_eq!(scaled(five, true), five * INSTRUMENTED_FACTOR);
    }
}

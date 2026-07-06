//! Pure-Rust quantitative diagnostics shared between research-time
//! (BacktestingEngine) and, in the future, live-time (SignalEngine) callers.
//!
//! No BacktestingEngine- or SignalEngine-specific dependencies — this crate
//! is deliberately data-in/data-out so both sides can share one
//! implementation instead of risking silent divergence.

pub mod variance_ratio;
pub mod ic;
pub mod quantile;
pub mod volatility_regime;

pub use variance_ratio::{variance_ratio as compute_variance_ratio, VrResult};
pub use ic::{compute_ic, IcResult};
pub use quantile::{quantile_analysis, QuantileBucket};
pub use volatility_regime::{volatility_tercile_regimes, VolatilityRegime};

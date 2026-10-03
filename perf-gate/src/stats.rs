//! Percentiles, medians and the bootstrap confidence interval of a ratio.

/// Nearest-rank percentile (`q` in `0..=1`) of unsorted samples.
pub fn percentile(samples: &[u64], q: f64) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = (q * sorted.len() as f64).ceil() as usize;
    Some(sorted[rank.clamp(1, sorted.len()) - 1] as f64)
}

/// Median of `values`; the mean of the middle pair for even lengths.
pub fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mid = sorted.len() / 2;
    Some(if sorted.len().is_multiple_of(2) {
        f64::midpoint(sorted[mid - 1], sorted[mid])
    } else {
        sorted[mid]
    })
}

/// Relative spread of `values`: (max - min) / median.
pub fn relative_spread(values: &[f64]) -> Option<f64> {
    let center = median(values)?;
    if center == 0.0 {
        return None;
    }
    let max = values.iter().copied().fold(f64::MIN, f64::max);
    let min = values.iter().copied().fold(f64::MAX, f64::min);
    Some((max - min) / center)
}

/// Deterministic SplitMix64; fixtures and resampling must not change when a
/// dependency changes its generator.
#[derive(Debug, Clone)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..bound` (`bound > 0`).
    pub fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound
    }
}

/// Bootstrap interval of `median(numerator) / median(denominator)`.
///
/// Each side is resampled with replacement independently, `resamples` times;
/// the interval is the central `confidence` share of the resulting ratios.
pub fn bootstrap_ratio_interval(
    numerator: &[f64],
    denominator: &[f64],
    resamples: usize,
    confidence: f64,
    seed: u64,
) -> Option<(f64, f64)> {
    if numerator.is_empty() || denominator.is_empty() || resamples == 0 {
        return None;
    }
    let mut rng = SplitMix64::new(seed);
    let mut ratios = Vec::with_capacity(resamples);
    let mut num = vec![0.0; numerator.len()];
    let mut den = vec![0.0; denominator.len()];
    for _ in 0..resamples {
        resample(numerator, &mut num, &mut rng);
        resample(denominator, &mut den, &mut rng);
        let (n, d) = (median(&num)?, median(&den)?);
        if d > 0.0 {
            ratios.push(n / d);
        }
    }
    if ratios.is_empty() {
        return None;
    }
    ratios.sort_by(f64::total_cmp);
    let tail = (1.0 - confidence) / 2.0;
    let last = ratios.len() - 1;
    let lo = ratios[((tail * last as f64).floor() as usize).min(last)];
    let hi = ratios[(((1.0 - tail) * last as f64).ceil() as usize).min(last)];
    Some((lo, hi))
}

fn resample(source: &[f64], into: &mut [f64], rng: &mut SplitMix64) {
    for slot in into.iter_mut() {
        *slot = source[rng.below(source.len() as u64) as usize];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_is_nearest_rank() {
        let samples: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&samples, 0.50), Some(50.0));
        assert_eq!(percentile(&samples, 0.99), Some(99.0));
        assert_eq!(percentile(&samples, 1.0), Some(100.0));
        assert_eq!(percentile(&[7], 0.99), Some(7.0));
        assert_eq!(percentile(&[], 0.5), None);
    }

    #[test]
    fn median_handles_even_and_odd() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&[4.0, 1.0, 2.0, 3.0]), Some(2.5));
        assert_eq!(median(&[]), None);
    }

    #[test]
    fn identical_sides_give_an_interval_around_one() {
        let side = [100.0, 102.0, 98.0, 101.0, 99.0];
        let (lo, hi) = bootstrap_ratio_interval(&side, &side, 2000, 0.95, 1).unwrap();
        assert!(lo <= 1.0 && hi >= 1.0, "{lo}..{hi}");
        assert!(hi < 1.05, "{hi}");
    }

    #[test]
    fn shifted_side_moves_the_interval() {
        let base = [100.0, 102.0, 98.0, 101.0, 99.0];
        let slow: Vec<f64> = base.iter().map(|v| v * 1.3).collect();
        let (lo, _) = bootstrap_ratio_interval(&slow, &base, 2000, 0.95, 1).unwrap();
        assert!(lo > 1.2, "{lo}");
    }

    #[test]
    fn bootstrap_is_deterministic() {
        let a = [1.0, 2.0, 3.0, 4.0];
        let b = [2.0, 2.5, 3.0, 3.5];
        assert_eq!(
            bootstrap_ratio_interval(&a, &b, 500, 0.95, 9),
            bootstrap_ratio_interval(&a, &b, 500, 0.95, 9)
        );
    }
}

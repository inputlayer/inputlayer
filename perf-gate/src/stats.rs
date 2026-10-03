//! Percentiles, medians and the confidence interval of a median.

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

/// Distribution-free confidence interval of the median of `values`: the
/// order statistics `[x(k), x(n+1-k)]` with the largest `k` whose binomial
/// coverage is at least `confidence`. `None` when even `[min, max]` covers
/// less, i.e. too few values for that confidence.
pub fn median_interval(values: &[f64], confidence: f64) -> Option<(f64, f64)> {
    let n = values.len();
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    // P(Binomial(n, 1/2) <= j), accumulated term by term.
    let total = 2f64.powi(i32::try_from(n).ok()?);
    let mut term = 1.0;
    let mut below = 0.0;
    let mut best = None;
    for k in 1..=n.div_ceil(2) {
        below += term / total;
        term = term * (n - (k - 1)) as f64 / k as f64;
        if 1.0 - 2.0 * below >= confidence {
            best = Some(k);
        } else {
            break;
        }
    }
    let k = best?;
    Some((sorted[k - 1], sorted[n - k]))
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
    fn median_interval_uses_binomial_order_statistics() {
        let six: Vec<f64> = (1..=6).map(f64::from).collect();
        // n = 6: only [min, max] reaches 95% (96.9%).
        assert_eq!(median_interval(&six, 0.95), Some((1.0, 6.0)));
        let ten: Vec<f64> = (1..=10).map(f64::from).collect();
        // n = 10: [x(2), x(9)] covers 97.9%, [x(3), x(8)] only 89.1%.
        assert_eq!(median_interval(&ten, 0.95), Some((2.0, 9.0)));
        assert_eq!(median_interval(&ten, 0.85), Some((3.0, 8.0)));
    }

    #[test]
    fn too_few_values_have_no_interval() {
        // n = 5: [min, max] covers 93.75% < 95%.
        assert_eq!(median_interval(&[1.0, 2.0, 3.0, 4.0, 5.0], 0.95), None);
        assert_eq!(median_interval(&[], 0.95), None);
    }

    #[test]
    fn one_outlier_per_side_is_tolerated_at_ten_rounds() {
        let mut values = vec![1.0, 1.01, 0.99, 1.0, 1.02, 0.98, 1.0, 1.01, 0.99, 9.0];
        values.push(0.1);
        values.remove(0);
        let (lo, hi) = median_interval(&values, 0.95).unwrap();
        assert!(lo >= 0.98 && hi <= 1.02, "{lo}..{hi}");
    }
}

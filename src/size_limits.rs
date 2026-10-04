//! Upper bounds on sizes a request supplies.
//!
//! A count, limit or capacity taken from a program (`top_k<K, ...>`,
//! `hnsw_nearest`'s `k` and `ef_search`, `lsh_probes`'s `num_probes`, an
//! index's `ef_construction`) must never size an allocation unchecked: an
//! allocation the address space cannot hold makes the allocator fail, and
//! Rust aborts the whole process before the per-query memory limit can act.
//!
//! So each size is checked twice. The parser rejects a literal over its bound
//! with a `validation` error that names the parameter and the bound
//! ([`check`]). The evaluator also bounds every allocation it sizes from such
//! a value by what the input can actually produce, so a size computed at run
//! time is safe too.

/// Largest `k` of `top_k<K, ...>` and `top_k_threshold<K, ...>`.
pub const MAX_TOP_K: usize = 1_000_000;

/// Largest `k` of `hnsw_nearest`.
pub const MAX_HNSW_K: usize = 10_000;

/// Largest `ef_search` of `hnsw_nearest` and of an HNSW index.
pub const MAX_EF_SEARCH: usize = 10_000;

/// Largest `ef_construction` of an HNSW index.
pub const MAX_EF_CONSTRUCTION: usize = 10_000;

/// Bits an LSH hash uses at most; more hyperplanes are ignored.
pub const MAX_LSH_BITS: usize = 62;

/// Largest `num_probes` of `lsh_probes` and `lsh_multi_probe`: every distinct
/// probe the generator can produce (Hamming distance up to 3 over
/// [`MAX_LSH_BITS`] bits).
pub const MAX_LSH_PROBES: usize = lsh_probe_count(MAX_LSH_BITS);

/// Largest vector an LSH function hashes. Its hyperplanes take
/// `bits * dimension` floats, cached.
pub const MAX_LSH_DIMENSION: usize = 16_384;

/// Largest string `concat` or `replace` builds; beyond it they return null.
pub const MAX_COMPUTED_STRING_BYTES: usize = 16 * 1024 * 1024;

/// How many distinct probes the LSH probe generators produce for `bits` hash
/// bits: the bucket itself plus every bucket 1, 2 or 3 bit flips away.
pub const fn lsh_probe_count(bits: usize) -> usize {
    let n = if bits > MAX_LSH_BITS {
        MAX_LSH_BITS
    } else {
        bits
    };
    let pairs = n * n.saturating_sub(1) / 2;
    let triples = pairs * n.saturating_sub(2) / 3;
    1 + n + pairs + triples
}

/// Check a request-supplied size against its bound.
pub fn check(what: &str, value: usize, max: usize) -> Result<usize, String> {
    if value > max {
        return Err(format!("{what} must be at most {max}, got {value}"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsh_probe_count_matches_the_generator() {
        for bits in [0, 1, 2, 3, 8, 16, 62, 63, 1000] {
            let generated = crate::vector_ops::lsh_probes(0, bits, usize::MAX).len();
            assert_eq!(lsh_probe_count(bits), generated, "bits={bits}");
        }
        assert_eq!(MAX_LSH_PROBES, 39_774);
    }

    #[test]
    fn check_names_the_parameter_and_bound() {
        assert_eq!(check("k", 10, 10), Ok(10));
        let err = check("hnsw_nearest: k", 11, 10).unwrap_err();
        assert_eq!(err, "hnsw_nearest: k must be at most 10, got 11");
    }
}

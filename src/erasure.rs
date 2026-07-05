//! Systematic Reed-Solomon erasure coding over GF(2^8).
//!
//! A chunk of data is striped into `k` equally sized data shards and
//! extended with `m` parity shards; any `k` of the `k + m` shards
//! reconstruct the original data. The generator matrix is a Vandermonde
//! matrix normalized so its top `k` rows are the identity, which makes the
//! code systematic (data shards hold the original bytes) and guarantees
//! every `k`-row submatrix is invertible.

use anyhow::{Result, bail};

/// GF(2^8) modulo the AES-friendly primitive polynomial x^8+x^4+x^3+x^2+1.
const GF_POLY: u16 = 0x11d;

/// `exp` is doubled so `exp[log a + log b]` never needs a modulo.
const fn build_tables() -> ([u8; 512], [u8; 256]) {
    let mut exp = [0u8; 512];
    let mut log = [0u8; 256];
    let mut x: u16 = 1;
    let mut i = 0;
    while i < 255 {
        exp[i] = x as u8;
        log[x as usize] = i as u8;
        x <<= 1;
        if x & 0x100 != 0 {
            x ^= GF_POLY;
        }
        i += 1;
    }
    while i < 512 {
        exp[i] = exp[i - 255];
        i += 1;
    }
    (exp, log)
}

const TABLES: ([u8; 512], [u8; 256]) = build_tables();

#[inline]
fn gf_mul(a: u8, b: u8) -> u8 {
    if a == 0 || b == 0 {
        return 0;
    }
    let (exp, log) = (&TABLES.0, &TABLES.1);
    exp[log[a as usize] as usize + log[b as usize] as usize]
}

#[inline]
fn gf_inv(a: u8) -> u8 {
    debug_assert!(a != 0, "zero has no inverse");
    let (exp, log) = (&TABLES.0, &TABLES.1);
    exp[255 - log[a as usize] as usize]
}

fn gf_pow(base: u8, mut power: usize) -> u8 {
    let mut result = 1u8;
    let mut base = base;
    while power > 0 {
        if power & 1 == 1 {
            result = gf_mul(result, base);
        }
        base = gf_mul(base, base);
        power >>= 1;
    }
    result
}

/// Inverts a square matrix over GF(2^8) with Gauss-Jordan elimination.
fn invert_matrix(mut matrix: Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>> {
    let n = matrix.len();
    let mut inverse = (0..n)
        .map(|i| {
            let mut row = vec![0u8; n];
            row[i] = 1;
            row
        })
        .collect::<Vec<_>>();

    for col in 0..n {
        let pivot = (col..n)
            .find(|&row| matrix[row][col] != 0)
            .ok_or_else(|| anyhow::anyhow!("matrix is singular"))?;
        matrix.swap(col, pivot);
        inverse.swap(col, pivot);

        let scale = gf_inv(matrix[col][col]);
        for value in matrix[col].iter_mut().chain(inverse[col].iter_mut()) {
            *value = gf_mul(*value, scale);
        }

        for row in 0..n {
            if row == col || matrix[row][col] == 0 {
                continue;
            }
            let factor = matrix[row][col];
            for j in 0..n {
                matrix[row][j] ^= gf_mul(factor, matrix[col][j]);
                inverse[row][j] ^= gf_mul(factor, inverse[col][j]);
            }
        }
    }
    Ok(inverse)
}

fn matrix_multiply(a: &[Vec<u8>], b: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let rows = a.len();
    let inner = b.len();
    let cols = b[0].len();
    let mut out = vec![vec![0u8; cols]; rows];
    for (i, out_row) in out.iter_mut().enumerate() {
        for (out_value, j) in out_row.iter_mut().zip(0..cols) {
            let mut acc = 0u8;
            for l in 0..inner {
                acc ^= gf_mul(a[i][l], b[l][j]);
            }
            *out_value = acc;
        }
    }
    out
}

#[derive(Debug, Clone)]
pub struct ErasureCoder {
    data_shards: usize,
    parity_shards: usize,
    /// `(data_shards + parity_shards) x data_shards`; the top square is the
    /// identity.
    matrix: Vec<Vec<u8>>,
}

impl ErasureCoder {
    pub fn new(data_shards: usize, parity_shards: usize) -> Result<Self> {
        if data_shards == 0 || parity_shards == 0 {
            bail!("data and parity shard counts must both be at least 1");
        }
        if data_shards + parity_shards > 255 {
            bail!("at most 255 total shards are supported");
        }

        let total = data_shards + parity_shards;
        let vandermonde = (0..total)
            .map(|row| {
                (0..data_shards)
                    .map(|col| gf_pow(row as u8, col))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let top_inverse = invert_matrix(vandermonde[..data_shards].to_vec())
            .expect("vandermonde top square is invertible");
        let matrix = matrix_multiply(&vandermonde, &top_inverse);

        Ok(Self {
            data_shards,
            parity_shards,
            matrix,
        })
    }

    pub fn data_shards(&self) -> usize {
        self.data_shards
    }

    pub fn parity_shards(&self) -> usize {
        self.parity_shards
    }

    pub fn total_shards(&self) -> usize {
        self.data_shards + self.parity_shards
    }

    /// Size of each shard for `data_len` bytes of input.
    pub fn shard_size(&self, data_len: usize) -> usize {
        data_len.div_ceil(self.data_shards)
    }

    /// Stripes `data` into `data_shards` zero-padded shards and computes the
    /// parity shards. Returns all `total_shards()` shards in index order.
    pub fn encode(&self, data: &[u8]) -> Vec<Vec<u8>> {
        let shard_size = self.shard_size(data.len()).max(1);
        let mut shards = Vec::with_capacity(self.total_shards());
        for i in 0..self.data_shards {
            let start = (i * shard_size).min(data.len());
            let end = ((i + 1) * shard_size).min(data.len());
            let mut shard = data[start..end].to_vec();
            shard.resize(shard_size, 0);
            shards.push(shard);
        }
        for parity in 0..self.parity_shards {
            let row = &self.matrix[self.data_shards + parity];
            let mut shard = vec![0u8; shard_size];
            for (coefficient, data_shard) in row.iter().zip(&shards) {
                if *coefficient == 0 {
                    continue;
                }
                for (out, input) in shard.iter_mut().zip(data_shard) {
                    *out ^= gf_mul(*coefficient, *input);
                }
            }
            shards.push(shard);
        }
        shards
    }

    /// Fills in every missing shard from any `data_shards` present ones.
    pub fn reconstruct(&self, shards: &mut [Option<Vec<u8>>]) -> Result<()> {
        if shards.len() != self.total_shards() {
            bail!(
                "expected {} shard slots, got {}",
                self.total_shards(),
                shards.len()
            );
        }
        if shards.iter().all(|shard| shard.is_some()) {
            return Ok(());
        }
        let present = shards
            .iter()
            .enumerate()
            .filter_map(|(index, shard)| shard.as_ref().map(|bytes| (index, bytes)))
            .take(self.data_shards)
            .collect::<Vec<_>>();
        if present.len() < self.data_shards {
            bail!(
                "need {} shards to reconstruct, only {} available",
                self.data_shards,
                shards.iter().filter(|shard| shard.is_some()).count()
            );
        }
        let shard_size = present[0].1.len();
        if present.iter().any(|(_, bytes)| bytes.len() != shard_size) {
            bail!("shards have inconsistent sizes");
        }

        // Rows of the generator matrix for the shards we have; inverting
        // them maps those shards back onto the original data shards.
        let submatrix = present
            .iter()
            .map(|(index, _)| self.matrix[*index].clone())
            .collect::<Vec<_>>();
        let decode = invert_matrix(submatrix)?;

        let mut data = Vec::with_capacity(self.data_shards);
        for row in 0..self.data_shards {
            match &shards[row] {
                Some(bytes) => data.push(bytes.clone()),
                None => {
                    let mut shard = vec![0u8; shard_size];
                    for ((_, bytes), coefficient) in present.iter().zip(&decode[row]) {
                        if *coefficient == 0 {
                            continue;
                        }
                        for (out, input) in shard.iter_mut().zip(*bytes) {
                            *out ^= gf_mul(*coefficient, *input);
                        }
                    }
                    data.push(shard);
                }
            }
        }

        for (index, slot) in shards.iter_mut().enumerate() {
            if slot.is_some() {
                continue;
            }
            if index < self.data_shards {
                *slot = Some(data[index].clone());
            } else {
                let row = &self.matrix[index];
                let mut shard = vec![0u8; shard_size];
                for (coefficient, data_shard) in row.iter().zip(&data) {
                    if *coefficient == 0 {
                        continue;
                    }
                    for (out, input) in shard.iter_mut().zip(data_shard) {
                        *out ^= gf_mul(*coefficient, *input);
                    }
                }
                *slot = Some(shard);
            }
        }
        Ok(())
    }

    /// Concatenates the data shards back into the original `data_len` bytes.
    pub fn join(&self, shards: &[Option<Vec<u8>>], data_len: usize) -> Result<Vec<u8>> {
        let mut data = Vec::with_capacity(data_len);
        for shard in shards.iter().take(self.data_shards) {
            let Some(bytes) = shard else {
                bail!("cannot join: missing data shard");
            };
            data.extend_from_slice(bytes);
        }
        if data.len() < data_len {
            bail!(
                "shards hold {} bytes, expected at least {data_len}",
                data.len()
            );
        }
        data.truncate(data_len);
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn patterned(len: usize) -> Vec<u8> {
        let mut state = 0x2545f4914f6cdd1du64;
        (0..len)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                (state >> 33) as u8
            })
            .collect()
    }

    /// Every subset of `m` erased shards must reconstruct exactly.
    fn erasure_combinations(total: usize, erase: usize) -> Vec<Vec<usize>> {
        let mut out = Vec::new();
        let mut stack = vec![(0usize, Vec::new())];
        while let Some((start, picked)) = stack.pop() {
            if picked.len() == erase {
                out.push(picked);
                continue;
            }
            for index in start..total {
                let mut next = picked.clone();
                next.push(index);
                stack.push((index + 1, next));
            }
        }
        out
    }

    #[test]
    fn gf_mul_matches_field_axioms() {
        for a in 1..=255u8 {
            assert_eq!(gf_mul(a, gf_inv(a)), 1, "a = {a}");
            assert_eq!(gf_mul(a, 1), a);
            assert_eq!(gf_mul(a, 0), 0);
        }
        // Spot-check distributivity on a sample of triples.
        for a in (1..=255u8).step_by(7) {
            for b in (1..=255u8).step_by(11) {
                for c in (1..=255u8).step_by(13) {
                    assert_eq!(gf_mul(a, b ^ c), gf_mul(a, b) ^ gf_mul(a, c));
                }
            }
        }
    }

    #[test]
    fn every_erasure_pattern_reconstructs() {
        for (k, m) in [(2usize, 1usize), (3, 2), (4, 2), (5, 3)] {
            let coder = ErasureCoder::new(k, m).unwrap();
            let data = patterned(k * 100 + 17);
            let encoded = coder.encode(&data);
            assert_eq!(encoded.len(), k + m);

            for erased in erasure_combinations(k + m, m) {
                let mut shards = encoded
                    .iter()
                    .cloned()
                    .map(Some)
                    .collect::<Vec<Option<Vec<u8>>>>();
                for index in &erased {
                    shards[*index] = None;
                }
                coder.reconstruct(&mut shards).unwrap();
                for (index, (shard, expected)) in shards.iter().zip(&encoded).enumerate() {
                    assert_eq!(
                        shard.as_ref().unwrap(),
                        expected,
                        "shard {index} after erasing {erased:?} with k={k} m={m}"
                    );
                }
                assert_eq!(coder.join(&shards, data.len()).unwrap(), data);
            }
        }
    }

    #[test]
    fn too_many_erasures_fail() {
        let coder = ErasureCoder::new(3, 2).unwrap();
        let mut shards = coder
            .encode(&patterned(300))
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        shards[0] = None;
        shards[2] = None;
        shards[4] = None;
        assert!(coder.reconstruct(&mut shards).is_err());
    }

    #[test]
    fn short_and_unaligned_inputs_round_trip() {
        let coder = ErasureCoder::new(4, 2).unwrap();
        for len in [0usize, 1, 3, 4, 5, 1023] {
            let data = patterned(len);
            let mut shards = coder
                .encode(&data)
                .into_iter()
                .map(Some)
                .collect::<Vec<_>>();
            shards[0] = None;
            shards[3] = None;
            coder.reconstruct(&mut shards).unwrap();
            assert_eq!(coder.join(&shards, len).unwrap(), data, "len = {len}");
        }
    }

    #[test]
    fn rejects_invalid_configurations() {
        assert!(ErasureCoder::new(0, 1).is_err());
        assert!(ErasureCoder::new(1, 0).is_err());
        assert!(ErasureCoder::new(200, 56).is_err());
        assert!(ErasureCoder::new(200, 55).is_ok());
    }
}

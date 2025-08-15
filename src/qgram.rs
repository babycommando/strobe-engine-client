// qgram.rs — 2-gram + 3-gram signatures (4096 bits) with fold(4096→512)

fn normalize(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        let c = ch.to_ascii_lowercase();
        if c.is_ascii_alphanumeric() || c == ' ' { out.push(c); } else { out.push(' '); }
    }
    out
}

#[inline(always)]
fn mix64_from3(b0: u8, b1: u8, b2: u8) -> u64 {
    const K0: u64 = 0x9e37_79b9_7f4a_7c15;
    let v = u64::from_le_bytes([b0, b1, b2, 0, 0, 0, 0, 0]);
    let mut a = K0 ^ v;
    a ^= a >> 33;
    a = a.wrapping_mul(0xff51_afd7_ed55_8ccd);
    a ^= a >> 33;
    a = a.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    a ^= a >> 33;
    a
}

/// Build a 4096-bit signature from BOTH 2-grams and 3-grams.
/// (Matches the browser’s logic; 4 projections per gram.)
pub fn sig4096_from_text(s: &str) -> [u64; 64] {
    let n = normalize(s);
    let b = n.as_bytes();
    let mut sig = [0u64; 64];

    // 3-grams
    if b.len() >= 3 {
        for i in 0..(b.len() - 2) {
            let mut x = mix64_from3(b[i], b[i + 1], b[i + 2]);
            for _ in 0..4 {
                let bit = (x & 0xFFF) as usize; // 0..4095
                unsafe { *sig.get_unchecked_mut(bit >> 6) |= 1u64 << (bit & 63); }
                x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            }
        }
    }

    // 2-grams
    if b.len() >= 2 {
        for i in 0..(b.len() - 1) {
            let mut x = mix64_from3(b[i], b[i + 1], 0);
            for _ in 0..4 {
                let bit = (x & 0xFFF) as usize;
                unsafe { *sig.get_unchecked_mut(bit >> 6) |= 1u64 << (bit & 63); }
                x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            }
        }
    }

    // Single-character query rescue
    if b.len() == 1 {
        let mut x = mix64_from3(b[0], 0, 0);
        for _ in 0..4 {
            let bit = (x & 0xFFF) as usize;
            unsafe { *sig.get_unchecked_mut(bit >> 6) |= 1u64 << (bit & 63); }
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        }
    }

    sig
}

/// Fold 4096 bits → 512 bits (XOR every 8th u64). Deterministic, very fast.
#[inline(always)]
pub fn fold4096_to_512(full: &[u64; 64]) -> [u64; 8] {
    let mut out = [0u64; 8];
    for i in 0..8 {
        out[i] = full[i]
            ^ full[i + 8]
            ^ full[i + 16]
            ^ full[i + 24]
            ^ full[i + 32]
            ^ full[i + 40]
            ^ full[i + 48]
            ^ full[i + 56];
    }
    out
}


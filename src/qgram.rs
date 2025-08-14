// qgram.rs — deterministic 3-gram -> 4096-bit signature (64*u64)
fn normalize(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        let c = ch.to_ascii_lowercase();
        if c.is_ascii_alphanumeric() || c == ' ' {
            out.push(c);
        } else {
            out.push(' ');
        }
    }
    out
}

pub fn qgrams3(s: &str) -> Vec<[u8; 3]> {
    let n = normalize(s);
    let bytes = n.as_bytes();
    let len = bytes.len();
    let mut out = Vec::with_capacity(len.saturating_sub(2));
    for i in 0..len.saturating_sub(2) {
        out.push([bytes[i], bytes[i + 1], bytes[i + 2]]);
    }
    out
}

#[inline(always)]
fn mix64_from_gram(g: [u8; 3]) -> u64 {
    const K0: u64 = 0x9e37_79b9_7f4a_7c15;
    let v = u64::from_le_bytes([g[0], g[1], g[2], 0, 0, 0, 0, 0]);
    let mut a = K0 ^ v;
    a ^= a >> 33;
    a = a.wrapping_mul(0xff51_afd7_ed55_8ccd);
    a ^= a >> 33;
    a = a.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    a ^= a >> 33;
    a
}

pub fn grams_to_sig4096<I: IntoIterator<Item = [u8; 3]>>(it: I) -> [u64; 64] {
    let mut sig = [0u64; 64];
    for g in it {
        let mut x = mix64_from_gram(g);
        for _ in 0..4 {
            let bit = (x & 0xFFF) as usize; // 0..4095
            sig[bit >> 6] |= 1u64 << (bit & 63);
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
        }
    }
    sig
}

// simd.rs — AVX2 nibble-LUT popcount with scalar fallback + 4096-bit support
use std::sync::Once;

#[derive(Copy, Clone, Debug)]
pub enum SimdPath {
    Avx2,
    Scalar,
}

static INIT: Once = Once::new();
static mut CHOSEN: SimdPath = SimdPath::Scalar;

pub fn init_and_log() {
    let p = chosen_path();
    println!("[strobe] SIMD path = {:?}", p);
}

#[inline]
pub fn chosen_path() -> SimdPath {
    INIT.call_once(|| unsafe {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx2") {
                CHOSEN = SimdPath::Avx2;
                return;
            }
        }
        CHOSEN = SimdPath::Scalar;
    });
    unsafe { CHOSEN }
}

// ----------------- Public API -----------------

#[inline(always)]
pub fn overlap_popcnt(a: &[u64; 4], b: &[u64; 4]) -> u32 {
    match chosen_path() {
        SimdPath::Avx2 => unsafe { overlap_popcnt_avx2(a, b) },
        SimdPath::Scalar => overlap_popcnt_scalar(a, b),
    }
}

#[inline(always)]
pub fn popcnt4(x: &[u64; 4]) -> u32 {
    match chosen_path() {
        SimdPath::Avx2 => unsafe { popcnt4_avx2(x) },
        SimdPath::Scalar => popcnt4_scalar(x),
    }
}

/// Popcount of (a & b) for 4096-bit signatures ([u64; 64]).
#[inline(always)]
pub fn popcnt4096_pair(a: &[u64; 64], b: &[u64; 64]) -> u32 {
    match chosen_path() {
        SimdPath::Avx2 => unsafe { popcnt4096_avx2(a, b) },
        SimdPath::Scalar => popcnt4096_scalar(a, b),
    }
}

/// Popcount of a single 4096-bit signature.
#[inline(always)]
pub fn popcnt4096_self(x: &[u64; 64]) -> u32 {
    match chosen_path() {
        SimdPath::Avx2 => unsafe { popcnt4096_avx2(x, &[u64::MAX; 64]) },
        SimdPath::Scalar => popcnt4096_scalar(x, &[u64::MAX; 64]),
    }
}

#[inline(always)]
pub fn prefetch_sig(ptr: *const u64) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_T0);
    }
}

// ----------------- Scalar fallback -----------------

#[inline(always)]
fn overlap_popcnt_scalar(a: &[u64; 4], b: &[u64; 4]) -> u32 {
    (a[0] & b[0]).count_ones()
        + (a[1] & b[1]).count_ones()
        + (a[2] & b[2]).count_ones()
        + (a[3] & b[3]).count_ones()
}

#[inline(always)]
fn popcnt4_scalar(x: &[u64; 4]) -> u32 {
    x[0].count_ones() + x[1].count_ones() + x[2].count_ones() + x[3].count_ones()
}

#[inline(always)]
fn popcnt4096_scalar(a: &[u64; 64], b: &[u64; 64]) -> u32 {
    let mut sum = 0;
    for i in 0..64 {
        sum += (a[i] & b[i]).count_ones();
    }
    sum
}

// ----------------- AVX2 path -----------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn overlap_popcnt_avx2(a: &[u64; 4], b: &[u64; 4]) -> u32 {
    use core::arch::x86_64::*;
    let va = _mm256_loadu_si256(a.as_ptr() as *const __m256i);
    let vb = _mm256_loadu_si256(b.as_ptr() as *const __m256i);
    let v = _mm256_and_si256(va, vb);
    popcnt256_bytesum(v)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn popcnt4_avx2(x: &[u64; 4]) -> u32 {
    use core::arch::x86_64::*;
    let v = _mm256_loadu_si256(x.as_ptr() as *const __m256i);
    popcnt256_bytesum(v)
}

/// AVX2 4096-bit popcount
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn popcnt4096_avx2(a: &[u64; 64], b: &[u64; 64]) -> u32 {
    use core::arch::x86_64::*;
    let mut sum = 0u32;
    let lut = _mm256_setr_epi8(
        0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,
        0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,
    );
    for i in (0..64).step_by(4) {
        let va = _mm256_loadu_si256(a[i..].as_ptr() as *const __m256i);
        let vb = _mm256_loadu_si256(b[i..].as_ptr() as *const __m256i);
        let v  = _mm256_and_si256(va, vb);
        let lo = _mm256_and_si256(v, _mm256_set1_epi8(0x0f));
        let hi = _mm256_and_si256(_mm256_srli_epi16(v, 4), _mm256_set1_epi8(0x0f));
        let cnt = _mm256_add_epi8(_mm256_shuffle_epi8(lut, lo), _mm256_shuffle_epi8(lut, hi));
        let sad  = _mm256_sad_epu8(cnt, _mm256_setzero_si256());
        let mut tmp = [0u64; 4];
        _mm256_storeu_si256(tmp.as_mut_ptr() as *mut _, sad);
        sum += (tmp[0] + tmp[1] + tmp[2] + tmp[3]) as u32;
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn popcnt256_bytesum(v: core::arch::x86_64::__m256i) -> u32 {
    use core::arch::x86_64::*;
    let lut = _mm256_setr_epi8(
        0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,
        0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,
    );
    let lo = _mm256_and_si256(v, _mm256_set1_epi8(0x0f));
    let hi = _mm256_and_si256(_mm256_srli_epi16(v, 4), _mm256_set1_epi8(0x0f));
    let cnt = _mm256_add_epi8(_mm256_shuffle_epi8(lut, lo), _mm256_shuffle_epi8(lut, hi));
    let sums = _mm256_sad_epu8(cnt, _mm256_setzero_si256());
    let mut tmp = [0u64; 4];
    _mm256_storeu_si256(tmp.as_mut_ptr() as *mut _, sums);
    (tmp[0] + tmp[1] + tmp[2] + tmp[3]) as u32
}

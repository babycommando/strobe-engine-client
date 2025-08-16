use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use crate::accum::Accum;
use crate::qgram::sig4096_from_text; // used by IndexBuilder
use crate::simd::{overlap_popcnt, popcnt4, prefetch_sig};
use crate::wire::{Hit, Query256, FLAG_FUZZY_JACCARD};

#[derive(Clone, Debug)]
pub struct DocMeta {
    pub id: u32,         // global ID (NOT searched)
    pub title: Arc<str>, // NOT searched (unless included in `search` field)
    pub author: Arc<str>,
    pub genres: Arc<str>,
    pub url: Arc<str>,
    pub uri: Arc<str>,
}

#[derive(Clone)]
pub struct Segment {
    // 256-bit (4×u64) SoA layout — minimal bytes per candidate
    pub s0: Arc<Vec<u64>>,
    pub s1: Arc<Vec<u64>>,
    pub s2: Arc<Vec<u64>>,
    pub s3: Arc<Vec<u64>>,
    // popcount for light normalization / fuzzy denominator
    pub pop: Arc<Vec<u16>>,
    // per-row metadata (immutable once sealed)
    pub meta: Arc<Vec<DocMeta>>,
    // postings over the first 256 bits
    pub bit_postings: Arc<[Vec<u32>; 256]>,
    pub bit_freq: Arc<[u32; 256]>,

    // -------- prefix & exact short-token postings --------
    // first-character postings (ASCII-lowered index by byte)
    pub pref1: Arc<[Vec<u32>; 256]>,
    // first-3-chars postings over base36 (a-z0-9) => 36^3 buckets
    pub pref3: Arc<Vec<Vec<u32>>>, // len = PREF3_SIZE
    // exact short tokens (<=6 chars), hashed; entries sorted by key
    pub full6: Arc<Vec<(u64, Vec<u32>)>>,
}

const INTERSECT_CAP: usize = 512;
const FUZZY_MIN_JACCARD: f32 = 0.05;

// prefix table sizes
const PREF3_BASE: usize = 36;
const PREF3_SIZE: usize = PREF3_BASE * PREF3_BASE * PREF3_BASE;

// scoring weights
const W_EXACT_LAST: f32 = 1000.0;   // huge: exact short token for the *last* token
const W_EXACT_ANY: f32 = 200.0;     // for other completed short tokens
const W_PREFIX_PER_CHAR: f32 = 30.0; // strong but below exact (e.g., 4 chars → +120)
const W_BOUNDARY: f32 = 0.25;       // tiny boundary-ish bump
const W_FUZZY_SCALE: f32 = 100.0;   // fuzzy weight when enabled

impl Segment {
    #[inline]
    pub fn len(&self) -> usize {
        self.s0.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.s0.is_empty()
    }
}

thread_local! {
    static SCRATCH: std::cell::RefCell<Scratch> = std::cell::RefCell::new(Scratch::new());
    static ACCUM_BEST: std::cell::RefCell<Accum> = std::cell::RefCell::new(Accum::default());
}

// We stash the latest raw query text in TLS before searching (main.rs sets it).
thread_local! {
    static QTEXT: std::cell::RefCell<String> = std::cell::RefCell::new(String::new());
}

/// Visible to main.rs
pub fn with_query_text<F, R>(s: &str, f: F) -> R
where
    F: FnOnce() -> R,
{
    QTEXT.with(|q| {
        let mut qb = q.borrow_mut();
        qb.clear();
        qb.push_str(s);
    });
    let r = f();
    QTEXT.with(|q| q.borrow_mut().clear());
    r
}

#[inline]
fn with_qtext<R>(f: impl FnOnce(&str) -> R) -> R {
    QTEXT.with(|q| f(&*q.borrow()))
}

struct Scratch {
    qbits: Vec<u16>,
    cand: Vec<u32>,
    tmp: Vec<u32>,
    qbuf: String, // normalized query text reuse
}
impl Scratch {
    fn new() -> Self {
        Self {
            qbits: Vec::with_capacity(64),
            cand: Vec::with_capacity(INTERSECT_CAP),
            tmp: Vec::with_capacity(INTERSECT_CAP),
            qbuf: String::with_capacity(256),
        }
    }
}

// ---------- helpers: ASCII normalization & tokenization ----------

#[inline]
fn to_lower_ascii(b: u8) -> u8 {
    if b'A' <= b && b <= b'Z' {
        b + 32
    } else {
        b
    }
}

#[inline]
fn is_alnum(b: u8) -> bool {
    (b'0'..=b'9').contains(&b) || (b'a'..=b'z').contains(&b) || (b'A'..=b'Z').contains(&b)
}

#[inline]
fn char36(b: u8) -> Option<u8> {
    let b = to_lower_ascii(b);
    if (b'a'..=b'z').contains(&b) {
        Some(b - b'a')
    } else if (b'0'..=b'9').contains(&b) {
        Some(26 + (b - b'0'))
    } else {
        None
    }
}

#[inline]
fn pref3_index(b0: u8, b1: u8, b2: u8) -> Option<usize> {
    let (x, y, z) = (char36(b0)?, char36(b1)?, char36(b2)?);
    let idx = (x as usize) * PREF3_BASE * PREF3_BASE + (y as usize) * PREF3_BASE + (z as usize);
    Some(idx)
}

#[inline]
fn normalize_ascii_inplace(s: &str, out: &mut String) {
    out.clear();
    out.reserve(s.len());
    for &b in s.as_bytes() {
        let c = to_lower_ascii(b);
        if is_alnum(c) || c == b' ' {
            out.push(c as char);
        } else {
            out.push(' ');
        }
    }
}

#[inline]
fn tokenize_bytes<'a>(bytes: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    let mut i = 0usize;
    while i < bytes.len() {
        while i < bytes.len() && !(is_alnum(bytes[i])) {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let start = i;
        i += 1;
        while i < bytes.len() && is_alnum(bytes[i]) {
            i += 1;
        }
        f(&bytes[start..i]);
    }
}

// simple 64-bit token hash (no deps)
#[inline]
fn hash_token64(tok: &[u8]) -> u64 {
    // xorshift-mix over bytes
    let mut h: u64 = 0x9E37_79B9_7F4A_7C15;
    for &b in tok {
        h ^= b as u64;
        h = h.rotate_left(13).wrapping_mul(0x9E37_0001_0000_0001);
    }
    h ^ (h >> 33)
}

#[inline]
fn contains_sorted(v: &[u32], x: u32) -> bool {
    let mut lo = 0usize;
    let mut hi = v.len();
    while lo < hi {
        let mid = (lo + hi) >> 1;
        let m = unsafe { *v.get_unchecked(mid) };
        if m < x {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo < v.len() && unsafe { *v.get_unchecked(lo) } == x
}

// ---------- public index view ----------

#[derive(Clone)]
pub struct IndexView {
    pub segments: Arc<[Arc<Segment>]>,
}
impl IndexView {
    pub fn from_segments(segments: Vec<Arc<Segment>>) -> Self {
        Self { segments: segments.into() }
    }
    pub fn total_docs(&self) -> usize {
        self.segments.iter().map(|s| s.len()).sum()
    }

    // main.rs sets TLS via with_query_text(...).
    pub fn search(&self, q: Query256) -> Vec<Hit> {
        if self.segments.is_empty() {
            return Vec::new();
        }
        // Collect per-segment candidates, then do global top-K selection.
        let mut pooled: Vec<Hit> = Vec::with_capacity(1024 * self.segments.len());
        for (seg_id, seg) in self.segments.iter().enumerate() {
            seg.search_into_v2(seg_id as u16, q, &mut pooled);
        }

        // Global top-K
        let k = q.k as usize;
        if pooled.len() <= k {
            // sort high → low
            pooled.sort_unstable_by(|a, b| b.cmp(a));
            return pooled;
        }
        let mut heap: BinaryHeap<Reverse<Hit>> = BinaryHeap::with_capacity(k);
        for h in pooled.into_iter() {
            if heap.len() < k {
                heap.push(Reverse(h));
            } else if h > heap.peek().unwrap().0 {
                heap.pop();
                heap.push(Reverse(h));
            }
        }
        let mut out = Vec::with_capacity(k);
        while let Some(Reverse(h)) = heap.pop() {
            out.push(h);
        }
        // return high → low
        out.sort_unstable_by(|a, b| b.cmp(a));
        out
    }
}

// =============== search implementation (prefix + exact + grams) ===============

impl Segment {
    pub fn search_into_v2(&self, seg_id: u16, q: Query256, out: &mut Vec<Hit>) {
        SCRATCH.with(|scratch| {
            let mut sc = scratch.borrow_mut();
            sc.qbits.clear();
            sc.cand.clear();
            sc.tmp.clear();
            sc.qbuf.clear();

            // ---- 0) Normalize raw query text once (no clones) ----
            with_qtext(|raw| normalize_ascii_inplace(raw, &mut sc.qbuf));

            // ---- 0b) Copy only the tokens we need; drop &qbuf borrows ----
            let (last_owned, others_short_owned): (Option<Vec<u8>>, Vec<Vec<u8>>) = {
                let mut tokens: Vec<Vec<u8>> = Vec::with_capacity(8);
                let b = sc.qbuf.as_bytes();
                tokenize_bytes(b, |t| tokens.push(t.to_vec()));
                let last = tokens.last().cloned();
                let mut others = Vec::with_capacity(4);
                if tokens.len() > 1 {
                    for t in &tokens[..tokens.len() - 1] {
                        if t.len() <= 6 && others.len() < 4 {
                            others.push(t.clone());
                        }
                    }
                }
                (last, others)
            };
            // no borrows into sc.qbuf past here

            // ---- 1) Gather rare q-bits ----
            for lane in 0..4 {
                let mut w = q.sig[lane];
                while w != 0 {
                    let tz = w.trailing_zeros() as u16;
                    sc.qbits.push(((lane as u16) << 6) | tz);
                    w &= w - 1;
                }
            }
            if self.is_empty() { return; }
            sc.qbits.sort_unstable_by_key(|&b| self.bit_freq[b as usize]);
            let grams_count = sc.qbits.len();

            // ---- 2) MUST set & exact slices ----
            let mut must_slice: &[u32] = &[];
            let mut last_prefix_len: usize = 0;
            if let Some(ref l) = last_owned {
                last_prefix_len = l.len();
                if l.len() >= 3 {
                    if let Some(idx) = pref3_index(l[0], l[1], l[2]) {
                        must_slice = unsafe { self.pref3.get_unchecked(idx) };
                    }
                } else if l.len() >= 1 {
                    must_slice = unsafe { self.pref1.get_unchecked(l[0] as usize) };
                }
            }

            let last_exact_slice = if let Some(ref l) = last_owned {
                if l.len() <= 6 {
                    let key = hash_token64(l);
                    lookup_full6(&self.full6, key)
                } else { None }
            } else { None };

            let mut other_exact_slices: [&[u32]; 4] = [&[]; 4];
            let mut ocount = 0usize;
            for t in &others_short_owned {
                if ocount == 4 { break; }
                let key = hash_token64(t);
                if let Some(v) = lookup_full6(&self.full6, key) {
                    other_exact_slices[ocount] = v;
                    ocount += 1;
                }
            }

            // ---- 3) Seed candidates ----
            if !must_slice.is_empty() {
                extend_cap(&mut sc.cand, must_slice, INTERSECT_CAP);
            } else if !sc.qbits.is_empty() {
                let rare0 = sc.qbits[0] as usize;
                if self.bit_freq[rare0] != 0 {
                    sc.cand.extend(self.bit_postings[rare0].iter().take(INTERSECT_CAP).copied());
                }
            }
            if sc.cand.is_empty() { return; }

            // Optional prune with a couple more rare bits
            let rare_bits_needed = if grams_count < 5 { 1 } else if grams_count < 10 { 2 } else { 3 };
            for bit_idx in 1..rare_bits_needed.min(sc.qbits.len()) {
                if sc.cand.is_empty() { break; }
                let bit = sc.qbits[bit_idx] as usize;
                if self.bit_freq[bit] != 0 {
                    // ---- FIX 1: move tmp out, use it, move back ----
                    let mut tmp_local = std::mem::take(&mut sc.tmp);
                    intersect_in_place_bounded_tmp(&mut sc.cand, &self.bit_postings[bit], INTERSECT_CAP, &mut tmp_local);
                    sc.tmp = tmp_local;
                }
            }

            // Hard enforce MUST (if set)
            if !must_slice.is_empty() {
                // ---- FIX 2: same trick here ----
                let mut tmp_local = std::mem::take(&mut sc.tmp);
                intersect_in_place_bounded_tmp(&mut sc.cand, must_slice, INTERSECT_CAP, &mut tmp_local);
                sc.tmp = tmp_local;
                if sc.cand.is_empty() { return; }
            }

            // ---- 4) Score candidates ----
            let qsig = [q.sig[0], q.sig[1], q.sig[2], q.sig[3]];
            let qpop = popcnt4(&qsig) as f32;
            let is_fuzzy = (q.flags & FLAG_FUZZY_JACCARD) != 0;

            ACCUM_BEST.with(|acc| {
                let mut acc = acc.borrow_mut();
                acc.begin();

                for (i, &row32) in sc.cand.iter().enumerate() {
                    let row = row32 as usize;

                    if i + 1 < sc.cand.len() {
                        let next_row = sc.cand[i + 1] as usize;
                        unsafe { prefetch_sig(self.s0.as_ptr().add(next_row)); }
                    }

                    let sig = [
                        unsafe { *self.s0.get_unchecked(row) },
                        unsafe { *self.s1.get_unchecked(row) },
                        unsafe { *self.s2.get_unchecked(row) },
                        unsafe { *self.s3.get_unchecked(row) },
                    ];
                    let inter = overlap_popcnt(&sig, &qsig) as f32;
                    if inter <= 0.0 { continue; }

                    let pb = self.pop[row] as f32;
                    let mut score_val = inter / (1.0 + 0.02 * pb);
                    score_val += W_BOUNDARY.min(inter * 0.02);

                    if last_prefix_len > 0 && !must_slice.is_empty() {
                        if contains_sorted(must_slice, row32) {
                            let l = last_prefix_len as f32;
                            score_val += (l.min(10.0)) * W_PREFIX_PER_CHAR;
                        }
                    }
                    if let Some(v) = last_exact_slice {
                        if contains_sorted(v, row32) {
                            score_val += W_EXACT_LAST;
                        }
                    }
                    for oi in 0..ocount {
                        let v = other_exact_slices[oi];
                        if !v.is_empty() && contains_sorted(v, row32) {
                            score_val += W_EXACT_ANY;
                            break;
                        }
                    }

                    if is_fuzzy {
                        let union = (qpop + pb - inter).max(1.0);
                        let jacc = inter / union;
                        if jacc >= FUZZY_MIN_JACCARD {
                            score_val += jacc * W_FUZZY_SCALE;
                        }
                    }

                    if acc.inc(row32) {
                        acc.set_score(row32, score_val);
                    } else {
                        acc.update_max(row32, score_val);
                    }
                }

                // local per-segment cap
                let mut heap = BinaryHeap::with_capacity(q.k as usize);
                for row32 in acc.iter_touched() {
                    let score = acc.get_score(row32);
                    let h = Hit { seg: seg_id, row: row32, score };
                    if heap.len() < q.k as usize {
                        heap.push(Reverse(h));
                    } else if score > heap.peek().unwrap().0.score {
                        heap.pop();
                        heap.push(Reverse(h));
                    }
                }
                while let Some(Reverse(h)) = heap.pop() {
                    out.push(h);
                }
            });
        });
    }
}


#[inline]
fn extend_cap(out: &mut Vec<u32>, src: &[u32], cap: usize) {
    let room = cap.saturating_sub(out.len());
    if room == 0 { return; }
    let take = room.min(src.len());
    out.extend_from_slice(&src[..take]);
}

#[inline]
fn lookup_full6<'a>(pairs: &'a Vec<(u64, Vec<u32>)>, key: u64) -> Option<&'a [u32]> {
    let mut lo = 0usize;
    let mut hi = pairs.len();
    while lo < hi {
        let mid = (lo + hi) >> 1;
        let k = unsafe { pairs.get_unchecked(mid).0 };
        if k < key { lo = mid + 1; } else { hi = mid; }
    }
    if lo < pairs.len() && pairs[lo].0 == key { Some(&pairs[lo].1) } else { None }
}

#[inline]
fn intersect_in_place_bounded_tmp(out: &mut Vec<u32>, b: &[u32], bound: usize, tmp: &mut Vec<u32>) {
    if out.is_empty() || b.is_empty() {
        out.clear();
        return;
    }
    let (mut i, mut j) = (0usize, 0usize);
    tmp.clear();
    tmp.reserve(out.len().min(bound));
    while i < out.len() && j < b.len() && tmp.len() < bound {
        let x = unsafe { *out.get_unchecked(i) };
        let y = unsafe { *b.get_unchecked(j) };
        if x == y {
            tmp.push(x);
            i += 1;
            j += 1;
        } else if x < y {
            i += 1;
        } else {
            j += 1;
        }
    }
    out.clear();
    out.extend_from_slice(tmp);
}

// ===================== builder =====================

pub struct IndexBuilder {
    pub(crate) meta: Vec<DocMeta>,
    signatures_aos: Vec<[u64; 4]>,
    id_to_row: std::collections::HashMap<u32, u32>,
    pop: Vec<u16>,
    since_seal: usize,
}
impl IndexBuilder {
    pub fn new() -> Self {
        Self {
            meta: Vec::with_capacity(64_000),
            signatures_aos: Vec::with_capacity(64_000),
            id_to_row: std::collections::HashMap::with_capacity(64_000),
            pop: Vec::with_capacity(64_000),
            since_seal: 0,
        }
    }
    #[inline]
    pub fn len(&self) -> usize { self.signatures_aos.len() }
    #[inline]
    pub fn docs_since_seal(&self) -> usize { self.since_seal }

    pub fn add(&mut self, item: crate::ingest::IngestItem) {
        let gid = item.id.unwrap_or(self.meta.len() as u32);

        // Search signature is built ONLY from `item.search`
        let full = sig4096_from_text(&item.search); // [u64; 64]
        let sig = [full[0], full[1], full[2], full[3]];
        let popcnt = popcnt4(&sig) as u16;

        if let Some(&row) = self.id_to_row.get(&gid) {
            let idx = row as usize;
            self.signatures_aos[idx] = sig;
            self.pop[idx] = popcnt;
            self.meta[idx] = DocMeta {
                id: gid,
                title: item.title.into(),
                author: item.author.into(),
                genres: item.genres.into(),
                url: item.url.into(),
                uri: item.uri.into(),
            };
        } else {
            let row = self.meta.len() as u32;
            self.id_to_row.insert(gid, row);
            self.signatures_aos.push(sig);
            self.pop.push(popcnt);
            self.meta.push(DocMeta {
                id: gid,
                title: item.title.into(),
                author: item.author.into(),
                genres: item.genres.into(),
                url: item.url.into(),
                uri: item.uri.into(),
            });
            self.since_seal += 1;
        }
    }

    pub fn seal_into_segment(&mut self) -> Segment {
        let n = self.signatures_aos.len();
        let mut postings: [Vec<u32>; 256] =
            std::array::from_fn(|_| Vec::with_capacity(n / 8 + 1));

        for (row, sig) in self.signatures_aos.iter().enumerate() {
            for lane in 0..4 {
                let mut w = sig[lane];
                while w != 0 {
                    let tz = w.trailing_zeros() as u16;
                    let bit = ((lane as u16) << 6) | tz;
                    postings[bit as usize].push(row as u32);
                    w &= w - 1;
                }
            }
        }

        let mut freq = [0u32; 256];
        for (i, v) in postings.iter_mut().enumerate() {
            v.sort_unstable();
            v.dedup();
            freq[i] = v.len() as u32;
        }

        let mut s0 = Vec::with_capacity(n);
        let mut s1 = Vec::with_capacity(n);
        let mut s2 = Vec::with_capacity(n);
        let mut s3 = Vec::with_capacity(n);
        for sig in &self.signatures_aos {
            s0.push(sig[0]);
            s1.push(sig[1]);
            s2.push(sig[2]);
            s3.push(sig[3]);
        }

        // -------- build prefix + exact short-token postings from meta --------
        let mut pref1: [Vec<u32>; 256] = std::array::from_fn(|_| Vec::new());
        let mut pref3: Vec<Vec<u32>> = (0..PREF3_SIZE).map(|_| Vec::new()).collect();
        let mut full6_map: std::collections::HashMap<u64, Vec<u32>> =
            std::collections::HashMap::new();

        let mut buf = String::new();
        for (row, m) in self.meta.iter().enumerate() {
            // Concatenate exactly what you consider searchable (title/author/genres)
            buf.clear();
            buf.push_str(&m.title);
            buf.push(' ');
            buf.push_str(&m.author);
            buf.push(' ');
            buf.push_str(&m.genres);

            let mut norm = String::new();
            normalize_ascii_inplace(&buf, &mut norm);
            let bytes = norm.as_bytes();

            tokenize_bytes(bytes, |tok| {
                if tok.is_empty() { return; }
                // pref1
                let c0 = tok[0] as usize;
                unsafe { pref1.get_unchecked_mut(c0) }.push(row as u32);

                // pref3
                if tok.len() >= 3 {
                    if let Some(idx) = pref3_index(tok[0], tok[1], tok[2]) {
                        unsafe { pref3.get_unchecked_mut(idx) }.push(row as u32);
                    }
                }

                // exact short (<=6)
                if tok.len() <= 6 {
                    let key = hash_token64(tok);
                    full6_map.entry(key).or_default().push(row as u32);
                }
            });
        }

        // sort/dedup all postings
        for v in pref1.iter_mut() {
            v.sort_unstable();
            v.dedup();
        }
        for v in pref3.iter_mut() {
            v.sort_unstable();
            v.dedup();
        }
        let mut full6_pairs: Vec<(u64, Vec<u32>)> = full6_map
            .into_iter()
            .map(|(k, mut v)| { v.sort_unstable(); v.dedup(); (k, v) })
            .collect();
        full6_pairs.sort_by_key(|p| p.0);

        let seg = Segment {
            s0: Arc::new(s0),
            s1: Arc::new(s1),
            s2: Arc::new(s2),
            s3: Arc::new(s3),
            pop: Arc::new(std::mem::take(&mut self.pop)),
            meta: Arc::new(std::mem::take(&mut self.meta)),
            bit_postings: Arc::new(postings),
            bit_freq: Arc::new(freq),

            pref1: Arc::new(pref1),
            pref3: Arc::new(pref3),
            full6: Arc::new(full6_pairs),
        };

        self.signatures_aos.clear();
        self.id_to_row.clear();
        self.since_seal = 0;

        seg
    }
}

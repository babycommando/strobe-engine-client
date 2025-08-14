use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use crate::accum::Accum;
use crate::qgram::{grams_to_sig4096, qgrams3};
use crate::simd::{overlap_popcnt, popcnt4, prefetch_sig};
use crate::wire::{Hit, Query4096, FLAG_FUZZY_JACCARD};

#[derive(Clone, Debug)]
pub struct DocMeta {
    pub id: u32,           // DISCO global ID (NOT searched)
    pub title: Arc<str>,   // NOT searched (unless included in `search` field)
    pub author: Arc<str>,  // NOT searched
    pub genres: Arc<str>,  // NOT searched
    pub url: Arc<str>,     // NOT searched
    pub uri: Arc<str>,     // NOT searched
}

#[derive(Clone)]
pub struct Segment {
    // 256-bit (4×u64) SoA layout — minimal bytes per candidate
    pub s0: Arc<Vec<u64>>,
    pub s1: Arc<Vec<u64>>,
    pub s2: Arc<Vec<u64>>,
    pub s3: Arc<Vec<u64>>,
    // popcount for Jaccard denominator
    pub pop: Arc<Vec<u16>>,
    // per-row metadata (immutable once sealed)
    pub meta: Arc<Vec<DocMeta>>,
    // postings over the first 256 bits
    pub bit_postings: Arc<[Vec<u32>; 256]>,
    pub bit_freq: Arc<[u32; 256]>,
}

const INTERSECT_CAP: usize = 512;
const MAX_RAREST_FOR_FUZZY: usize = 4;
const FUZZY_MIN_JACCARD: f32 = 0.05;

impl Segment {
    #[inline] pub fn len(&self) -> usize { self.s0.len() }
    #[inline] pub fn is_empty(&self) -> bool { self.s0.is_empty() }

    pub fn search_into(&self, seg_id: u16, q: Query4096, out: &mut Vec<Hit>) {
        SCRATCH.with(|scratch| {
            let mut sc = scratch.borrow_mut();
            sc.qbits.clear();
            sc.cand.clear();

            // collect set bits from first 256 query bits
            for lane in 0..4 {
                let mut w = q.sig[lane];
                while w != 0 {
                    let tz = w.trailing_zeros() as u16;
                    sc.qbits.push(((lane as u16) << 6) | tz);
                    w &= w - 1;
                }
            }
            if sc.qbits.is_empty() || self.is_empty() { return; }

            sc.qbits.sort_unstable_by_key(|&b| self.bit_freq[b as usize]);

            let grams_count = sc.qbits.len();
            let rare_bits_needed = if grams_count < 5 { 1 } else if grams_count < 10 { 2 } else { 3 };
            let rare_bits_needed = rare_bits_needed.min(sc.qbits.len());

            // seed
            let rare0 = sc.qbits[0] as usize;
            if self.bit_freq[rare0] == 0 { return; }
            sc.cand.extend(self.bit_postings[rare0].iter().take(INTERSECT_CAP));

            // intersect a few more rare bits
            for bit_idx in 1..rare_bits_needed {
                let bit = sc.qbits[bit_idx] as usize;
                if self.bit_freq[bit] != 0 {
                    intersect_in_place_bounded(&mut sc.cand, &self.bit_postings[bit], INTERSECT_CAP);
                    if sc.cand.is_empty() { return; }
                }
            }

            let is_fuzzy = (q.flags & FLAG_FUZZY_JACCARD) != 0;
            if is_fuzzy && sc.cand.len() > INTERSECT_CAP {
                let upto = MAX_RAREST_FOR_FUZZY.min(sc.qbits.len());
                let mut used = rare_bits_needed;
                while sc.cand.len() > INTERSECT_CAP && used < upto {
                    let bit = sc.qbits[used] as usize;
                    if self.bit_freq[bit] != 0 {
                        intersect_in_place_bounded(&mut sc.cand, &self.bit_postings[bit], INTERSECT_CAP);
                        if sc.cand.is_empty() { break; }
                    }
                    used += 1;
                }
            }

            let qsig = [q.sig[0], q.sig[1], q.sig[2], q.sig[3]];
            let qpop = popcnt4(&qsig) as f32;

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

                    if is_fuzzy {
                        let pb = self.pop[row] as f32;
                        let union = (qpop + pb - inter).max(1.0);
                        let jacc = inter / union;

                        let score_cutoff = if grams_count <= 4 { FUZZY_MIN_JACCARD * 0.5 } else { FUZZY_MIN_JACCARD };
                        if jacc < score_cutoff { continue; }

                        let score_val = jacc * 1000.0 + inter * 0.001;
                        if acc.inc(row32) { acc.set_score(row32, score_val); } else { acc.update_max(row32, score_val); }
                    } else {
                        let score_val = inter;
                        if acc.inc(row32) { acc.set_score(row32, score_val); } else { acc.update_max(row32, score_val); }
                    }
                }

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

thread_local! {
    static SCRATCH: std::cell::RefCell<Scratch> = std::cell::RefCell::new(Scratch::new());
    static ACCUM_BEST: std::cell::RefCell<Accum> = std::cell::RefCell::new(Accum::default());
}

struct Scratch {
    qbits: Vec<u16>,
    cand: Vec<u32>,
}
impl Scratch {
    fn new() -> Self { Self { qbits: Vec::with_capacity(64), cand: Vec::with_capacity(INTERSECT_CAP) } }
}

#[inline]
fn intersect_in_place_bounded(out: &mut Vec<u32>, b: &Vec<u32>, bound: usize) {
    if out.is_empty() || b.is_empty() { out.clear(); return; }
    let (mut i, mut j) = (0usize, 0usize);
    let mut next = Vec::with_capacity(out.len().min(bound));
    while i < out.len() && j < b.len() && next.len() < bound {
        let x = unsafe { *out.get_unchecked(i) };
        let y = unsafe { *b.get_unchecked(j) };
        if x == y { next.push(x); i += 1; j += 1; }
        else if x < y { i += 1; }
        else { j += 1; }
    }
    out.clear();
    out.extend_from_slice(&next);
}

#[derive(Clone)]
pub struct IndexView {
    pub segments: Arc<[Arc<Segment>]>,
}
impl IndexView {
    pub fn from_segments(segments: Vec<Arc<Segment>>) -> Self { Self { segments: segments.into() } }
    pub fn total_docs(&self) -> usize { self.segments.iter().map(|s| s.len()).sum() }
    pub fn search(&self, q: Query4096) -> Vec<Hit> {
        if self.segments.is_empty() { return Vec::new(); }
        let mut pooled: Vec<Hit> = Vec::with_capacity(1024 * self.segments.len());
        for (seg_id, seg) in self.segments.iter().enumerate() {
            seg.search_into(seg_id as u16, q, &mut pooled);
        }
        pooled
    }
}

pub struct IndexBuilder {
    meta: Vec<DocMeta>,
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
    #[inline] pub fn len(&self) -> usize { self.signatures_aos.len() }
    #[inline] pub fn docs_since_seal(&self) -> usize { self.since_seal }

    pub fn add(&mut self, item: crate::ingest::IngestItem) {
        let gid = item.id.unwrap_or(self.meta.len() as u32);

        // Search signature is built ONLY from `item.search` (NOT id/uri/etc.)
        let full = grams_to_sig4096(qgrams3(&item.search)); // [u64; 64]
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
        let mut postings: [Vec<u32>; 256] = std::array::from_fn(|_| Vec::with_capacity(n / 8 + 1));

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
            s0.push(sig[0]); s1.push(sig[1]); s2.push(sig[2]); s3.push(sig[3]);
        }

        let seg = Segment {
            s0: Arc::new(s0),
            s1: Arc::new(s1),
            s2: Arc::new(s2),
            s3: Arc::new(s3),
            pop: Arc::new(std::mem::take(&mut self.pop)),
            meta: Arc::new(std::mem::take(&mut self.meta)),
            bit_postings: Arc::new(postings),
            bit_freq: Arc::new(freq),
        };

        self.signatures_aos.clear();
        self.id_to_row.clear();
        self.since_seal = 0;

        seg
    }
}

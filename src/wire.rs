use bytes::BytesMut;

// bit 0 -> enable fuzzy re-rank (Jaccard on 4096-bit signatures)
pub const FLAG_FUZZY_JACCARD: u16 = 1 << 0;
// bit 1 -> include metadata (title, author, genres, url, uri) in response
pub const FLAG_WITH_META: u16 = 1 << 1;

// 4096 bits = 64 u64 values → 64 * 8 bytes = 512 bytes
// plus k: u16 (2 bytes), flags: u16 (2 bytes) = 4 bytes
// total size = 516 bytes
pub const QUERY_LEN: usize = 516;

#[derive(Clone, Copy)]
pub struct Query4096 {
    pub k: u16,
    pub flags: u16,
    pub sig: [u64; 64],
}

impl Query4096 {
    pub fn from_bytes(b: &[u8]) -> Self {
        debug_assert_eq!(b.len(), QUERY_LEN);
        let k = u16::from_le_bytes([b[0], b[1]]);
        let flags = u16::from_le_bytes([b[2], b[3]]);
        let mut sig = [0u64; 64];
        for i in 0..64 {
            let start = 4 + i * 8;
            sig[i] = u64::from_le_bytes(b[start..start + 8].try_into().unwrap());
        }
        Self { k, flags, sig }
    }
}

// Internal hit: (segment id, row) + score
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct Hit {
    pub seg: u16,
    pub row: u32,
    pub score: f32,
}

impl Eq for Hit {}
impl Ord for Hit {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| self.row.cmp(&other.row))
            .then_with(|| self.seg.cmp(&other.seg))
    }
}

// Encode hits. If with_meta=true, append 5 strings per hit.
pub fn encode_hits_binary(
    view: &crate::index::IndexView,
    hits: &[Hit],
    with_meta: bool,
) -> bytes::Bytes {
    let mut buf = BytesMut::with_capacity(4 + hits.len() * (4 + 4 + 10));
    buf.extend_from_slice(&(hits.len() as u32).to_le_bytes());

    for h in hits {
        let seg = &view.segments[h.seg as usize];
        let row = h.row as usize;
        let gid = seg.meta[row].id;

        // id + score (always present)
        buf.extend_from_slice(&gid.to_le_bytes());
        buf.extend_from_slice(&h.score.to_le_bytes());

        if with_meta {
            let m = &seg.meta[row];

            let tb = (&*m.title).as_bytes();
            let ab = (&*m.author).as_bytes();
            let gb = (&*m.genres).as_bytes();
            let ub = (&*m.url).as_bytes();
            let rb = (&*m.uri).as_bytes();

            let tl = (tb.len().min(u16::MAX as usize)) as u16;
            let al = (ab.len().min(u16::MAX as usize)) as u16;
            let gl = (gb.len().min(u16::MAX as usize)) as u16;
            let ul = (ub.len().min(u16::MAX as usize)) as u16;
            let rl = (rb.len().min(u16::MAX as usize)) as u16;

            buf.extend_from_slice(&tl.to_le_bytes());
            buf.extend_from_slice(&al.to_le_bytes());
            buf.extend_from_slice(&gl.to_le_bytes());
            buf.extend_from_slice(&ul.to_le_bytes());
            buf.extend_from_slice(&rl.to_le_bytes());

            buf.extend_from_slice(&tb[..tl as usize]);
            buf.extend_from_slice(&ab[..al as usize]);
            buf.extend_from_slice(&gb[..gl as usize]);
            buf.extend_from_slice(&ub[..ul as usize]);
            buf.extend_from_slice(&rb[..rl as usize]);
        }
    }

    buf.freeze()
}

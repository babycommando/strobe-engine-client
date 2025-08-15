use flume::Sender;
use std::time::Duration;

#[derive(Debug)]
pub struct IngestItem {
    pub id: Option<u32>, // if None or u32::MAX => assign sequentially
    pub search: String,  // SEARCHABLE: e.g., "title author genres"
    // METADATA (NOT searchable unless you include in `search`)
    pub title: String,
    pub author: String,
    pub genres: String,
    pub url: String,
    pub uri: String,
}

#[inline]
fn enqueue(tx: &Sender<IngestItem>, item: IngestItem) -> anyhow::Result<()> {
    tx.send_timeout(item, Duration::from_millis(100))
        .map_err(|e| anyhow::anyhow!(e.to_string()))
}

// Legacy line ingest: "id<TAB>search\n" or "search\n" (meta empty)
pub fn ingest_lines(tx: &Sender<IngestItem>, body: &[u8]) -> anyhow::Result<usize> {
    let mut n = 0usize;
    for line in body.split(|&b| b == b'\n') {
        if line.is_empty() { continue; }
        if let Some(tab) = line.iter().position(|&b| b == b'\t') {
            let id = unsafe { std::str::from_utf8_unchecked(&line[..tab]) }
                .trim().parse::<u32>().ok();
            let search = unsafe { std::str::from_utf8_unchecked(&line[tab + 1..]) }
                .trim().to_string();
            if !search.is_empty() {
                enqueue(tx, IngestItem { id, search, title:String::new(), author:String::new(), genres:String::new(), url:String::new(), uri:String::new() })?;
                n += 1;
            }
        } else {
            let search = unsafe { std::str::from_utf8_unchecked(line) }.trim().to_string();
            if !search.is_empty() {
                enqueue(tx, IngestItem { id: None, search, title:String::new(), author:String::new(), genres:String::new(), url:String::new(), uri:String::new() })?;
                n += 1;
            }
        }
    }
    Ok(n)
}

// Legacy binary ingest: [u32 id][u32 len][len bytes UTF-8 search] (meta empty)
pub fn ingest_bin(tx: &Sender<IngestItem>, body: &[u8]) -> anyhow::Result<usize> {
    let mut i = 0usize;
    let mut n = 0usize;
    while i + 8 <= body.len() {
        let id = u32::from_le_bytes([body[i], body[i+1], body[i+2], body[i+3]]); i += 4;
        let len = u32::from_le_bytes([body[i], body[i+1], body[i+2], body[i+3]]) as usize; i += 4;
        if i + len > body.len() { break; }
        let search = unsafe { String::from_utf8_unchecked(body[i..i+len].to_vec()) }; i += len;
        let id_opt = if id == u32::MAX { None } else { Some(id) };
        enqueue(tx, IngestItem {
            id: id_opt, search, title:String::new(), author:String::new(), genres:String::new(), url:String::new(), uri:String::new()
        })?;
        n += 1;
    }
    Ok(n)
}

/// NEW atomic packed ingest: repeated records
/// [u32 id]
/// [u16 sl][u16 tl][u16 al][u16 gl][u16 ul][u16 rl]
/// [search][title][author][genres][url][uri]
pub fn parse_ingest_pack(body: &[u8]) -> anyhow::Result<Vec<IngestItem>> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 14 <= body.len() {
        let id = u32::from_le_bytes([body[i], body[i+1], body[i+2], body[i+3]]); i += 4;
        let sl = u16::from_le_bytes([body[i], body[i+1]]) as usize; i += 2;
        let tl = u16::from_le_bytes([body[i], body[i+1]]) as usize; i += 2;
        let al = u16::from_le_bytes([body[i], body[i+1]]) as usize; i += 2;
        let gl = u16::from_le_bytes([body[i], body[i+1]]) as usize; i += 2;
        let ul = u16::from_le_bytes([body[i], body[i+1]]) as usize; i += 2;
        let rl = u16::from_le_bytes([body[i], body[i+1]]) as usize; i += 2;
        let need = sl + tl + al + gl + ul + rl;
        if i + need > body.len() { break; }

        let search = unsafe { String::from_utf8_unchecked(body[i..i+sl].to_vec()) }; i += sl;
        let title  = unsafe { String::from_utf8_unchecked(body[i..i+tl].to_vec()) }; i += tl;
        let author = unsafe { String::from_utf8_unchecked(body[i..i+al].to_vec()) }; i += al;
        let genres = unsafe { String::from_utf8_unchecked(body[i..i+gl].to_vec()) }; i += gl;
        let url    = unsafe { String::from_utf8_unchecked(body[i..i+ul].to_vec()) }; i += ul;
        let uri    = unsafe { String::from_utf8_unchecked(body[i..i+rl].to_vec()) }; i += rl;

        out.push(IngestItem {
            id: if id == u32::MAX { None } else { Some(id) },
            search, title, author, genres, url, uri,
        });
    }
    Ok(out)
}


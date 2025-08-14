use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
};

/// WAL sync behavior (same semantics as your original)
#[derive(Clone, Copy, Debug)]
pub enum SyncMode {
    Always,
    CoalesceBytes(usize),
    Never,
}

/// Atomic WAL for packed ingest:
/// Repeated record:
/// [u32 id][u16 sl][u16 tl][u16 al][u16 gl][u16 ul][u16 rl]
/// [search][title][author][genres][url][uri]
pub struct PackWal {
    path: PathBuf,
    f: File,
    unsynced: usize,
    sync: SyncMode,
}

impl PackWal {
    pub fn open(dir: &Path, shard_id: usize, sync: SyncMode) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("shard{}.pack", shard_id));
        let f = OpenOptions::new().create(true).append(true).read(true).open(&path)?;
        Ok(Self { path, f, unsynced: 0, sync })
    }

    #[inline]
    pub fn append_pack(
        &mut self,
        id: u32,
        search: &[u8],
        title: &[u8],
        author: &[u8],
        genres: &[u8],
        url: &[u8],
        uri: &[u8],
    ) -> std::io::Result<()> {
        self.f.write_all(&id.to_le_bytes())?;
        self.f.write_all(&(search.len() as u16).to_le_bytes())?;
        self.f.write_all(&(title.len()  as u16).to_le_bytes())?;
        self.f.write_all(&(author.len() as u16).to_le_bytes())?;
        self.f.write_all(&(genres.len() as u16).to_le_bytes())?;
        self.f.write_all(&(url.len()    as u16).to_le_bytes())?;
        self.f.write_all(&(uri.len()    as u16).to_le_bytes())?;
        self.f.write_all(search)?; self.f.write_all(title)?; self.f.write_all(author)?;
        self.f.write_all(genres)?; self.f.write_all(url)?;   self.f.write_all(uri)?;

        self.unsynced += 4 + 12 + search.len() + title.len() + author.len() + genres.len() + url.len() + uri.len();
        match self.sync {
            SyncMode::Always => { self.f.sync_data()?; self.unsynced = 0; }
            SyncMode::CoalesceBytes(thresh) => {
                if self.unsynced >= thresh { self.f.sync_data()?; self.unsynced = 0; }
            }
            SyncMode::Never => {}
        }
        Ok(())
    }

    pub fn reader(&self) -> std::io::Result<PackReader> {
        let rf = OpenOptions::new().read(true).open(&self.path)?;
        Ok(PackReader { br: BufReader::new(rf) })
    }
}

pub struct PackReader {
    br: BufReader<File>,
}

pub struct PackRec {
    pub id: u32,
    pub search: Vec<u8>,
    pub title: Vec<u8>,
    pub author: Vec<u8>,
    pub genres: Vec<u8>,
    pub url: Vec<u8>,
    pub uri: Vec<u8>,
}

impl PackReader {
    pub fn next(&mut self) -> std::io::Result<Option<PackRec>> {
        let mut idb = [0u8; 4];
        match self.br.read_exact(&mut idb) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let id = u32::from_le_bytes(idb);

        let mut lens = [0u8; 12];
        if let Err(e) = self.br.read_exact(&mut lens) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof { return Ok(None); }
            return Err(e);
        }
        let sl = u16::from_le_bytes([lens[0], lens[1]]) as usize;
        let tl = u16::from_le_bytes([lens[2], lens[3]]) as usize;
        let al = u16::from_le_bytes([lens[4], lens[5]]) as usize;
        let gl = u16::from_le_bytes([lens[6], lens[7]]) as usize;
        let ul = u16::from_le_bytes([lens[8], lens[9]]) as usize;
        let rl = u16::from_le_bytes([lens[10], lens[11]]) as usize;

        let mut search = vec![0u8; sl];
        let mut title  = vec![0u8; tl];
        let mut author = vec![0u8; al];
        let mut genres = vec![0u8; gl];
        let mut url    = vec![0u8; ul];
        let mut uri    = vec![0u8; rl];

        if sl > 0 { self.br.read_exact(&mut search)?; }
        if tl > 0 { self.br.read_exact(&mut title)?; }
        if al > 0 { self.br.read_exact(&mut author)?; }
        if gl > 0 { self.br.read_exact(&mut genres)?; }
        if ul > 0 { self.br.read_exact(&mut url)?; }
        if rl > 0 { self.br.read_exact(&mut uri)?; }

        Ok(Some(PackRec { id, search, title, author, genres, url, uri }))
    }
}

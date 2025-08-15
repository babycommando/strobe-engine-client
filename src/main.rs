// main.rs — h2/h1 + CORS + binary search payload with trailing raw query text
// Wire: [36 bytes fixed][u16 qlen][qlen bytes utf-8]

use std::{
    env,
    fs::File,
    io::BufReader,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use bytes::Bytes;
use flume::{Receiver, Sender};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming as HBody, header, Method, Request, Response, StatusCode};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;

use tokio_rustls::TlsAcceptor;
use rustls::ServerConfig;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

mod qgram;
mod simd;
mod accum;
mod storage;
mod ingest;
mod index;
mod wire;

use index::{IndexBuilder, IndexView, Segment, with_query_text};
use wire::{Query256, encode_hits_binary, QUERY_FIXED_LEN, FLAG_WITH_META};

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

struct AppState {
    view: Arc<ArcSwap<IndexView>>,
    tx: Sender<ingest::IngestItem>,
    shards: usize,
    shard_id: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let bind: SocketAddr = env::var("BIND").unwrap_or_else(|_| "0.0.0.0:7700".into()).parse()?;
    let shards: usize = env::var("SHARDS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    let shard_id: usize = env::var("SHARD_ID").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let data_dir = env::var("DATA_DIR").unwrap_or_else(|_| "./data".into());
    let mode = env::var("MODE").unwrap_or_else(|_| "h1".into());

    // TLS args
    let mut cert_path = env::var("CERT").unwrap_or_else(|_| "cert.pem".into());
    let mut key_path  = env::var("KEY").unwrap_or_else(|_| "key.pem".into());
    {
        let mut args = std::env::args();
        while let Some(arg) = args.next() {
            if arg == "--cert" { if let Some(v) = args.next() { cert_path = v; } }
            else if arg == "--key" { if let Some(v) = args.next() { key_path = v; } }
        }
    }

    // SIMD path
    crate::simd::init_and_log();

    let (tx, rx) = flume::bounded::<ingest::IngestItem>(65_536);

    // WAL sync policy
    let sync_mode = match env::var("WAL_SYNC").as_deref() {
        Ok("always") => storage::SyncMode::Always,
        Ok(s) if s.starts_with("coalesce:") => {
            let n = s.split(':').nth(1).unwrap().parse().unwrap_or(1 << 20);
            storage::SyncMode::CoalesceBytes(n)
        }
        Ok("never") => storage::SyncMode::Never,
        _ => storage::SyncMode::CoalesceBytes(1 << 20),
    };

    // Boot replay from atomic pack WAL
    let mut wal = storage::PackWal::open(std::path::Path::new(&data_dir), shard_id, sync_mode)?;
    let replay_seg_docs: usize = env::var("REPLAY_SEG_DOCS").ok().and_then(|s| s.parse().ok()).unwrap_or(200_000);

    let mut boot_builder = IndexBuilder::new();
    let mut segments: Vec<Arc<Segment>> = Vec::new();
    if let Ok(mut rdr) = wal.reader() {
        while let Some(rec) = rdr.next()? {
            let item = ingest::IngestItem {
                id: Some(rec.id),
                search: unsafe { String::from_utf8_unchecked(rec.search) },
                title:  unsafe { String::from_utf8_unchecked(rec.title) },
                author: unsafe { String::from_utf8_unchecked(rec.author) },
                genres: unsafe { String::from_utf8_unchecked(rec.genres) },
                url:    unsafe { String::from_utf8_unchecked(rec.url) },
                uri:    unsafe { String::from_utf8_unchecked(rec.uri) },
            };
            boot_builder.add(item);
            if boot_builder.len() >= replay_seg_docs {
                let seg = Arc::new(boot_builder.seal_into_segment());
                segments.push(seg);
            }
        }
    }
    if boot_builder.len() > 0 {
        let seg = Arc::new(boot_builder.seal_into_segment());
        segments.push(seg);
    }
    let view0 = Arc::new(ArcSwap::from_pointee(IndexView::from_segments(segments)));

    let app = Arc::new(AppState { view: view0.clone(), tx: tx.clone(), shards, shard_id });

    // Builder loop
    let flush_docs: usize = env::var("FLUSH_DOCS").ok().and_then(|s| s.parse().ok()).unwrap_or(4096);
    let flush_ms: u64    = env::var("FLUSH_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(5);
    tokio::spawn(builder_loop(app.clone(), rx, wal, flush_docs, flush_ms));

    match mode.as_str() {
        "h1" => run_h1_plain(app.clone(), bind).await?,
        "h2c" => run_h2c(app.clone(), bind).await?,
        "h2" => run_h2_tls(app.clone(), bind, cert_path, key_path).await?,
        _ => panic!("Unknown MODE '{}'", mode),
    }
    Ok(())
}

// ------------- minimal CORS helpers -------------
#[inline]
fn add_cors<B>(mut resp: Response<B>) -> Response<B> {
    use hyper::header;
    let h = resp.headers_mut();
    h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, header::HeaderValue::from_static("*"));
    h.insert(header::ACCESS_CONTROL_ALLOW_METHODS, header::HeaderValue::from_static("GET,POST,OPTIONS"));
    h.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, header::HeaderValue::from_static("Content-Type"));
    h.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, header::HeaderValue::from_static("X-Ingested"));
    h.insert(header::ACCESS_CONTROL_MAX_AGE, header::HeaderValue::from_static("600"));
    resp
}

#[inline]
fn cors_no_content() -> Response<Full<Bytes>> {
    add_cors(
        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
}
// ------------------------------------------------

async fn run_h1_plain(app: Arc<AppState>, bind: SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind).await?;
    println!("[strobe] h1-plain on http://{}", bind);
    println!("[strobe] shards={} shard_id={}", app.shards, app.shard_id);

    loop {
        let (mut stream, _) = listener.accept().await?;
        let _ = stream.set_nodelay(true);
        let app = app.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| handle(req, app.clone()));
            let conn = hyper::server::conn::http1::Builder::new()
                .keep_alive(true)
                .pipeline_flush(true)
                .serve_connection(io, svc);
            if let Err(e) = conn.await { eprintln!("[conn] {}", e); }
        });
    }
}

async fn run_h2c(app: Arc<AppState>, bind: SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind).await?;
    println!("[strobe] h2c on http://{}", bind);
    println!("[strobe] shards={} shard_id={}", app.shards, app.shard_id);

    loop {
        let (mut stream, _) = listener.accept().await?;
        let _ = stream.set_nodelay(true);
        let app = app.clone();

        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let mut h2 = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
            h2.max_concurrent_streams(Some(1_000_000))
              .initial_connection_window_size(Some(1 << 30))
              .initial_stream_window_size(Some(1 << 20));
            let svc = service_fn(move |req| handle(req, app.clone()));
            if let Err(e) = h2.serve_connection(io, svc).await { eprintln!("[conn] {}", e); }
        });
    }
}

async fn run_h2_tls(
    app: Arc<AppState>,
    bind: SocketAddr,
    cert_path: String,
    key_path: String,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind).await?;
    println!("[strobe] h2-tls on https://{}", bind);
    println!("[strobe] shards={} shard_id={}", app.shards, app.shard_id);

    // Load leaf cert
    let certs: Vec<CertificateDer<'static>> = {
        let mut rd = BufReader::new(File::open(&cert_path)?);
        let v = rustls_pemfile::certs(&mut rd).collect::<Result<Vec<_>, _>>()?;
        if v.is_empty() { anyhow::bail!("no certificates found in {}", cert_path); }
        v
    };
    // Load matching PKCS#8 key
    let key_der: PrivateKeyDer<'static> = {
        let mut rd = BufReader::new(File::open(&key_path)?);
        let mut keys = rustls_pemfile::pkcs8_private_keys(&mut rd)
            .collect::<Result<Vec<PrivatePkcs8KeyDer<'static>>, _>>()?;
        if keys.is_empty() { anyhow::bail!("no PKCS#8 private keys found in {}", key_path); }
        let pkcs8: PrivatePkcs8KeyDer<'static> = keys.remove(0);
        PrivateKeyDer::from(pkcs8)
    };
    let mut cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key_der)?;
    cfg.alpn_protocols = vec![b"h2".to_vec()];
    let acceptor = TlsAcceptor::from(Arc::new(cfg));

    loop {
        let (mut stream, _) = listener.accept().await?;
        let _ = stream.set_nodelay(true);
        let app = app.clone();
        let acceptor = acceptor.clone();

        tokio::spawn(async move {
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    let io = TokioIo::new(tls_stream);
                    let mut h2 = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
                    h2.max_concurrent_streams(Some(1_000_000))
                      .initial_connection_window_size(Some(1 << 30))
                      .initial_stream_window_size(Some(1 << 20));
                    let svc = service_fn(move |req| handle(req, app.clone()));
                    if let Err(e) = h2.serve_connection(io, svc).await { eprintln!("[conn] {}", e); }
                }
                Err(e) => eprintln!("[tls] handshake error: {}", e),
            }
        });
    }
}

async fn builder_loop(
    app: Arc<AppState>,
    rx: Receiver<ingest::IngestItem>,
    mut wal: storage::PackWal,
    flush_docs: usize,
    flush_ms: u64,
) {
    let mut last_flush = Instant::now();
    let mut builder = IndexBuilder::new();
    let mut next_id: u32 = 0;

    loop {
        let mut took = 0usize;
        for _ in 0..8192 {
            match rx.try_recv() {
                Ok(mut it) => {
                    // assign sequential id if needed (auto-id == u32::MAX)
                    let gid = it.id.unwrap_or_else(|| { let x = next_id; next_id = x.wrapping_add(1); x });
                    if gid == u32::MAX { continue; } // never allow sentinel

                    // atomic WAL append (search + metadata)
                    if let Err(e) = wal.append_pack(
                        gid,
                        it.search.as_bytes(),
                        it.title.as_bytes(),
                        it.author.as_bytes(),
                        it.genres.as_bytes(),
                        it.url.as_bytes(),
                        it.uri.as_bytes(),
                    ) {
                        eprintln!("[wal] append error: {}", e);
                        continue;
                    }
                    // feed builder
                    it.id = Some(gid);
                    builder.add(it);
                    took += 1;
                }
                Err(flume::TryRecvError::Empty) => break,
                Err(flume::TryRecvError::Disconnected) => return,
            }
        }

        let docs_since = builder.docs_since_seal();
        let timed_out = last_flush.elapsed() >= Duration::from_millis(flush_ms);
        if docs_since > 0 && (docs_since >= flush_docs || timed_out) {
            let seg = Arc::new(builder.seal_into_segment());
            let mut next: Vec<Arc<Segment>> = app.view.load().segments.to_vec();
            let total_before = next.iter().map(|s| s.len()).sum::<usize>();
            next.push(seg.clone());
            let total_after = total_before + seg.len();
            app.view.store(Arc::new(IndexView::from_segments(next)));
            last_flush = Instant::now();
            println!("[segment] published: +{} docs (total {})", seg.len(), total_after);
        }

        if took == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

// ========== /search helpers: read binary payload with optional trailing text ==========

#[inline(always)]
fn parse_query_and_text(buf: &[u8]) -> Result<(Query256, &str), ()> {
    if buf.len() < QUERY_FIXED_LEN { return Err(()); }
    let q = Query256::from_bytes(&buf[..QUERY_FIXED_LEN]);

    if buf.len() == QUERY_FIXED_LEN {
        return Ok((q, "")); // no raw text attached
    }
    if buf.len() < QUERY_FIXED_LEN + 2 { return Err(()); }
    let ql_lo = buf[QUERY_FIXED_LEN];
    let ql_hi = buf[QUERY_FIXED_LEN + 1];
    let qlen = u16::from_le_bytes([ql_lo, ql_hi]) as usize;
    if buf.len() < QUERY_FIXED_LEN + 2 + qlen { return Err(()); }
    let s = unsafe { std::str::from_utf8_unchecked(&buf[QUERY_FIXED_LEN + 2 .. QUERY_FIXED_LEN + 2 + qlen]) };
    Ok((q, s))
}

async fn handle(req: Request<HBody>, app: Arc<AppState>) -> anyhow::Result<Response<Full<Bytes>>> {
    match (req.method(), req.uri().path()) {
        // ---- CORS preflight ----
        (&Method::OPTIONS, _) => Ok(cors_no_content()),

        (&Method::GET, "/proto") => {
            let resp = Response::builder().status(200)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Full::new(Bytes::from_static(b"h1-plain"))).unwrap();
            Ok(add_cors(resp))
        }
        (&Method::GET, "/stats") => {
            let v = app.view.load();
            let msg = format!("segments={}\ndocs_total={}\n", v.segments.len(), v.total_docs());
            let resp = Response::builder().status(200)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Full::new(Bytes::from(msg))).unwrap();
            Ok(add_cors(resp))
        }

        // legacy endpoints (still supported)
        (&Method::POST, "/ingest") => {
            let body = req.into_body().collect().await?.to_bytes();
            let count = ingest::ingest_lines(&app.tx, &body)?;
            let resp = Response::builder().status(StatusCode::ACCEPTED)
                .header("X-Ingested", count.to_string())
                .header(header::CONNECTION, "keep-alive")
                .body(Full::new(Bytes::new())).unwrap();
            Ok(add_cors(resp))
        }
        (&Method::POST, "/ingest.bin") => {
            let body = req.into_body().collect().await?.to_bytes();
            let count = ingest::ingest_bin(&app.tx, &body)?;
            let resp = Response::builder().status(StatusCode::ACCEPTED)
                .header("X-Ingested", count.to_string())
                .header(header::CONNECTION, "keep-alive")
                .body(Full::new(Bytes::new())).unwrap();
            Ok(add_cors(resp))
        }

        // NEW atomic packed ingest
        (&Method::POST, "/ingest.pack") => {
            let body = req.into_body().collect().await?.to_bytes();
            let items = ingest::parse_ingest_pack(&body)?;
            let mut ok = 0usize;
            for it in items {
                if app.tx.send_async(it).await.is_ok() { ok += 1; } else { break; }
            }
            let resp = Response::builder().status(StatusCode::ACCEPTED)
                .header("X-Ingested", ok.to_string())
                .header(header::CONNECTION, "keep-alive")
                .body(Full::new(Bytes::new())).unwrap();
            Ok(add_cors(resp))
        }

        // ======== search: [36 fixed][u16 qlen][qlen utf8] ========
        (&Method::POST, "/search") => {
            let body = req.into_body().collect().await?.to_bytes();
            let (q, qtext) = match parse_query_and_text(&body) {
                Ok(v) => v,
                Err(_) => {
                    let resp = Response::builder().status(StatusCode::BAD_REQUEST)
                        .header(header::CONNECTION, "keep-alive")
                        .body(Full::new(Bytes::from_static(b"bad query payload"))).unwrap();
                    return Ok(add_cors(resp));
                }
            };

            let view = app.view.load();
            // Stash raw query text in TLS for prefix/exact scoring in index.rs
            let hits = with_query_text(qtext, || view.search(q));
            let with_meta = (q.flags & FLAG_WITH_META) != 0;
            let bytes = encode_hits_binary(&view, &hits, with_meta);

            let resp = Response::builder().status(200)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONNECTION, "keep-alive")
                .body(Full::new(bytes)).unwrap();
            Ok(add_cors(resp))
        }

        _ => {
            let resp = Response::builder().status(404)
                .header(header::CONNECTION, "keep-alive")
                .body(Full::new(Bytes::new())).unwrap();
            Ok(add_cors(resp))
        }
    }
}

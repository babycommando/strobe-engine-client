#!/usr/bin/env python3
import random, struct, sys, threading, time
from queue import Queue, Full, Empty
from pathlib import Path
import httpx

# ==== CONFIG ====
URL = "https://127.0.0.1:7700/ingest.pack"
VERIFY_TLS = False
TOTAL = 2_000_000
BATCH_SIZE = 5000
WORKERS = 4
SEED = 1337
YEAR_RANGE = (1960, 2025)
AUTO_IDS = False

# ==== LOAD REAL WORDS ====
def load_real_words():
    words = set()
    dict_paths = [
        Path("/usr/share/dict/words"),
        Path("/usr/dict/words"),
    ]
    for dp in dict_paths:
        if dp.exists():
            with open(dp, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    w = line.strip().lower()
                    if len(w) >= 3 and w.isalpha():
                        words.add(w)
    if not words:
        # fallback words
        words.update(["neon", "city", "dreams", "echo", "night", "machine", "ocean", "storm", "velvet", "pulse"])
    vocab = sorted(words)
    random.shuffle(vocab)
    return vocab

# ==== RECORD GEN ====
def make_metadata(r, words, yrange):
    title = f"{r.choice(words).capitalize()} {r.choice(words).capitalize()}"
    author = f"{r.choice(words).capitalize()} {r.choice(words).capitalize()}"
    genres = " ".join(r.sample(words, 3))
    url = f"https://example.com/{r.choice(words)}"
    uri = f"disco://{r.choice(words)}/{r.choice(words)}"
    search = f"{title} {author} {genres}"
    return search, title, author, genres, url, uri

def pack_ingest_record(doc_id, search, title, author, genres, url, uri):
    sb, tb, ab, gb, ub, rb = [s.encode("utf-8") for s in (search, title, author, genres, url, uri)]
    return (
        struct.pack("<I6H", doc_id, len(sb), len(tb), len(ab), len(gb), len(ub), len(rb)) +
        sb + tb + ab + gb + ub + rb
    )

# ==== PRODUCER ====
def producer(total, batch_size, q, seed, yrange, auto_ids, words):
    r = random.Random(seed)
    next_id = 0
    while next_id < total:
        buf = bytearray()
        count = min(batch_size, total - next_id)
        for _ in range(count):
            doc_id = 0xFFFFFFFF if auto_ids else next_id
            search, title, author, genres, url, uri = make_metadata(r, words, yrange)
            buf += pack_ingest_record(doc_id, search, title, author, genres, url, uri)
            next_id += 1
        while True:
            try:
                q.put((count, bytes(buf)), timeout=1)
                break
            except Full:
                pass
    q.put(None)

# ==== CONSUMER ====
def consumer(url, verify_tls, q, stats, total, max_retries=5):
    headers = {"Content-Type": "application/octet-stream"}
    with httpx.Client(http2=True, verify=verify_tls, timeout=30.0) as client:
        while True:
            try:
                item = q.get(timeout=1)
            except Empty:
                continue
            if item is None:
                q.put(None)
                return
            batch_count, payload = item
            attempt = 0
            while attempt <= max_retries:
                try:
                    r = client.post(url, content=payload, headers=headers)
                    ok = (200 <= r.status_code < 300)
                except httpx.RequestError:
                    ok = False
                if ok:
                    stats["batches"] += 1
                    stats["bytes"] += len(payload)
                    stats["records"] += batch_count
                    stats["ok"] += 1
                    if stats["batches"] % 10 == 0:
                        pct = (stats["records"]/total)*100
                        rate = stats["records"] / (time.perf_counter()-stats["t_start"])
                        sys.stdout.write(f"[progress] {stats['records']}/{total} ({pct:.2f}%) ok={stats['ok']} err={stats['err']} rate={rate:.1f} rec/s\n")
                        sys.stdout.flush()
                    break
                else:
                    attempt += 1
                    if attempt > max_retries:
                        stats["err"] += 1
                    else:
                        time.sleep(0.5 * attempt)

# ==== MAIN ====
def main():
    words = load_real_words()
    yrange = YEAR_RANGE
    q = Queue(maxsize=32)
    stats = {"batches":0,"bytes":0,"records":0,"ok":0,"err":0,"t_start":time.perf_counter()}

    prod = threading.Thread(target=producer, args=(TOTAL,BATCH_SIZE,q,SEED,yrange,AUTO_IDS,words), daemon=True)
    prod.start()

    consumers = []
    for _ in range(WORKERS):
        t = threading.Thread(target=consumer, args=(URL, VERIFY_TLS, q, stats, TOTAL), daemon=True)
        t.start()
        consumers.append(t)

    prod.join()
    for t in consumers:
        t.join()

    elapsed = time.perf_counter()-stats["t_start"]
    print(f"[done] {stats['records']}/{TOTAL} recs in {elapsed:.1f}s ({stats['records']/elapsed:.1f} rec/s) ok={stats['ok']} err={stats['err']}")

if __name__ == "__main__":
    main()

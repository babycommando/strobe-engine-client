#!/usr/bin/env python3
import random, struct, sys, threading, time
from queue import Queue, Full, Empty
from pathlib import Path
import httpx  # HTTP/2-capable client

# ==== CONFIG ====
URL = "https://127.0.0.1:7700/ingest.bin"  # Hardcoded endpoint
VERIFY_TLS = False                     # Self-signed allowed
TOTAL = 2_000_000
BATCH_SIZE = 5000
WORKERS = 4
SEED = 1337
YEAR_RANGE = (1960, 2025)
AUTO_IDS = False                       # True = server auto IDs

# ==== VOCAB ====
def load_vocab():
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
    for i in range(500_000):
        words.add(f"tok{i}")
    for i in range(100_000):
        words.add(hex(random.getrandbits(32))[2:])
    vocab = sorted(words)
    random.shuffle(vocab)
    return vocab

def split_vocab(vocab):
    random.shuffle(vocab)
    n = len(vocab)
    slice_size = n // 6
    return (
        vocab[0:slice_size],
        vocab[slice_size:2*slice_size],
        vocab[2*slice_size:3*slice_size],
        vocab[3*slice_size:4*slice_size],
        vocab[4*slice_size:5*slice_size],
        vocab[5*slice_size:],
    )

# ==== RECORD GEN ====
def make_text(doc_id, r, adj, noun, verb, art1, art2, misc, year_range):
    return f"{r.choice(adj)} {r.choice(noun)} {r.choice(verb)} — {r.choice(art1)} {r.choice(art2)} ({r.randint(year_range[0], year_range[1])}) genre:{r.choice(misc)} tags:{' '.join(r.sample(misc, 5))} | {r.choice(misc)}"

def pack_record(doc_id, text_utf8):
    return struct.pack("<II", doc_id, len(text_utf8)) + text_utf8

# ==== PRODUCER ====
def producer(total, batch_size, q, seed, yrange, auto_ids, adj, noun, verb, art1, art2, misc):
    r = random.Random(seed)
    next_id = 0
    while next_id < total:
        buf = bytearray()
        count = min(batch_size, total - next_id)
        for _ in range(count):
            doc_id = 0xFFFFFFFF if auto_ids else next_id
            text = make_text(next_id, r, adj, noun, verb, art1, art2, misc, yrange).encode("utf-8")
            buf += pack_record(doc_id, text)
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
    vocab = load_vocab()
    adj, noun, verb, art1, art2, misc = split_vocab(vocab)
    yrange = YEAR_RANGE

    q = Queue(maxsize=32)
    stats = {"batches":0,"bytes":0,"records":0,"ok":0,"err":0,"t_start":time.perf_counter()}

    prod = threading.Thread(target=producer, args=(TOTAL,BATCH_SIZE,q,SEED,yrange,AUTO_IDS,adj,noun,verb,art1,art2,misc), daemon=True)
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

#!/usr/bin/env python3
import argparse, os, random, sys, threading, time, struct, requests
from queue import Queue, Full, Empty
from requests.exceptions import RequestException

# -----------------------
# DATA POOLS
# -----------------------
ADJ = ["Electric","Neon","Dark","Velvet","Psychedelic","Frozen","Digital","Golden","Burning","Mystic"]
NOUN = ["Dreams","Storm","Echoes","City","Machine","Revolution","Night","Sky","Fire","Ocean"]
VERB = ["Burning","Rising","Falling","Dancing","Shining","Crashing"]
ART1 = ["DJ","MC","The","Captain","Saint","Professor"]
ART2 = ["Phoenix","Shadow","Machine","Groove","Echo","Vision"]
GENRES = ["techno","acid","electro","house","breaks"]
SUBGENRES = ["raw","hypnotic","groovy","melodic","deep"]
TAGS = ["club","festival","underground","afterhours","warehouse"]
BLURB = ["recorded live in berlin","sleazy warehouse cut","drone-laden roller"]

# -----------------------
# RECORD GENERATORS
# -----------------------
def rand_title(r): return f"{r.choice(ADJ)} {r.choice(NOUN)} {r.choice(VERB)}"
def rand_artist(r): return f"{r.choice(ART1)} {r.choice(ART2)}"
def rand_year(r, yr): return r.randint(yr[0], yr[1])
def rand_genre(r): return f"{r.choice(GENRES)}:{r.choice(SUBGENRES)}"
def rand_tags(r, n=3): return " ".join(r.sample(TAGS, r.randint(2, n)))
def rand_blurb(r): return r.choice(BLURB)

def make_text(doc_id, r, yr):
    return f"{rand_title(r)} — {rand_artist(r)} ({rand_year(r, yr)}) genre:{rand_genre(r)} tags:{rand_tags(r)} | {rand_blurb(r)}"

def pack_record(doc_id, text_utf8):
    return struct.pack("<II", doc_id, len(text_utf8)) + text_utf8

# -----------------------
# PRODUCER
# -----------------------
def producer(total, batch_size, q, seed, yrange, auto_ids):
    r = random.Random(seed)
    next_id = 0
    while next_id < total:
        buf = bytearray()
        count = min(batch_size, total - next_id)
        for _ in range(count):
            doc_id = 0xFFFFFFFF if auto_ids else next_id
            text = make_text(next_id, r, yrange).encode("utf-8")
            buf += pack_record(doc_id, text)
            next_id += 1
        while True:
            try:
                q.put((count, bytes(buf)), timeout=1)
                break
            except Full:
                pass
    q.put(None)

# -----------------------
# CONSUMER (with retry)
# -----------------------
def consumer(url, verify_tls, q, session, stats, total, max_retries=5):
    headers = {"Content-Type": "application/octet-stream"}
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
            t0 = time.perf_counter()
            try:
                r = session.post(url, data=payload, headers=headers, verify=verify_tls, timeout=30)
                if 200 <= r.status_code < 300:
                    ok = True
                else:
                    ok = False
                    sys.stderr.write(f"[HTTP {r.status_code}] attempt={attempt}\n")
            except RequestException as e:
                ok = False
                sys.stderr.write(f"[error] attempt={attempt} {e}\n")

            if ok:
                dt = time.perf_counter() - t0
                stats["batches"] += 1
                stats["bytes"] += len(payload)
                stats["records"] += batch_count
                stats["ok"] += 1
                if stats["batches"] % 10 == 0:
                    pct = (stats["records"]/total)*100
                    rate = stats["records"] / (time.perf_counter()-stats["t_start"])
                    sys.stdout.write(
                        f"[progress] {stats['records']}/{total} ({pct:.2f}%) "
                        f"ok={stats['ok']} err={stats['err']} rate={rate:.1f} rec/s\n"
                    )
                    sys.stdout.flush()
                break
            else:
                attempt += 1
                if attempt > max_retries:
                    stats["err"] += 1
                    sys.stderr.write(f"[drop] batch lost after {max_retries} retries\n")
                else:
                    time.sleep(0.5 * attempt)  # backoff

# -----------------------
# MAIN
# -----------------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://127.0.0.1:7700/ingest.bin")
    p.add_argument("--total", type=int, default=2_000_000)
    p.add_argument("--batch", type=int, default=5000)
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--seed", type=int, default=1337)
    p.add_argument("--year-min", type=int, default=1960)
    p.add_argument("--year-max", type=int, default=2025)
    p.add_argument("--auto-ids", action="store_true")
    p.add_argument("--insecure", action="store_true")
    return p.parse_args()

def main():
    args = parse_args()
    yrange = (args.year_min, args.year_max)
    q = Queue(maxsize=32)
    stats = {"batches":0,"bytes":0,"records":0,"ok":0,"err":0,"t_start":time.perf_counter()}

    prod = threading.Thread(target=producer, args=(args.total,args.batch,q,args.seed,yrange,args.auto_ids), daemon=True)
    prod.start()

    consumers = []
    for _ in range(args.workers):
        s = requests.Session()
        t = threading.Thread(target=consumer, args=(args.url, not args.insecure, q, s, stats, args.total), daemon=True)
        t.start()
        consumers.append(t)

    prod.join()
    for t in consumers:
        t.join()

    elapsed = time.perf_counter()-stats["t_start"]
    print(f"[done] {stats['records']}/{args.total} recs in {elapsed:.1f}s "
          f"({stats['records']/elapsed:.1f} rec/s) ok={stats['ok']} err={stats['err']}")

if __name__ == "__main__":
    main()

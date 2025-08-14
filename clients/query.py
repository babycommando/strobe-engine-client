#!/usr/bin/env python3
import struct, requests
from typing import List

# ---------- ahash-lite (same as before) ----------
K0 = 0x9e3779b97f4a7c15
K1 = 0x6a09e667f3bcc909

def ahash64(data: bytes) -> int:
    a = K0
    v = int.from_bytes(data.ljust(8, b"\0"), "little")
    a ^= v
    a = (a ^ (a >> 33)) * 0xff51afd7ed558ccd & 0xFFFFFFFFFFFFFFFF
    a = (a ^ (a >> 33)) * 0xc4ceb9fe1a85ec53 & 0xFFFFFFFFFFFFFFFF
    a ^= a >> 33
    return a & 0xFFFFFFFFFFFFFFFF

def normalize(s: str) -> str:
    out = []
    for ch in s:
        c = ch.lower()
        out.append(c if (c.isalnum() or c == " ") else " ")
    return "".join(out)

def qgrams3(s: str) -> List[bytes]:
    b = normalize(s).encode("ascii", "ignore")
    return [b[i:i+3] for i in range(len(b) - 2)]

def grams_to_sig256(grams: List[bytes]) -> List[int]:
    sig = [0, 0, 0, 0]
    for g in grams:
        x = ahash64(g)
        for _ in range(4):
            bit = x & 0xFF
            sig[bit >> 6] |= 1 << (bit & 63)
            x ^= (x << 13) & 0xFFFFFFFFFFFFFFFF
            x ^= (x >> 7) & 0xFFFFFFFFFFFFFFFF
            x ^= (x << 17) & 0xFFFFFFFFFFFFFFFF
    return sig

# ---------- wire helpers ----------
FLAG_FUZZY_JACCARD = 1 << 0  # matches server

def make_query_bytes(text: str, k: int = 5, fuzzy: bool = False) -> bytes:
    s0, s1, s2, s3 = grams_to_sig256(qgrams3(text))
    flags = FLAG_FUZZY_JACCARD if fuzzy else 0
    return struct.pack("<HH" + "Q"*64, k, flags, *sig)

def ingest_text(url: str, text: str):
    b = text.encode("utf-8")
    payload = struct.pack("<II", 0xFFFFFFFF, len(b)) + b
    r = requests.post(url.rstrip("/") + "/ingest.bin",
                      data=payload,
                      headers={"Content-Type": "application/octet-stream"})
    print(f"[ingest] status={r.status_code} X-Ingested={r.headers.get('X-Ingested')}")

def search_text(url: str, text: str, k: int = 5, fuzzy: bool = False):
    q = make_query_bytes(text, k=k, fuzzy=fuzzy)
    r = requests.post(url.rstrip("/") + "/search", data=q)
    data = r.content
    if len(data) < 4:
        print("[search] bad response"); return
    (hit_count,) = struct.unpack_from("<I", data, 0)
    print(f"[search] '{text}' k={k} fuzzy={int(fuzzy)} -> {hit_count} hits")
    off = 4
    for i in range(hit_count):
        doc_id, score = struct.unpack_from("<If", data, off)
        print(f"  {i+1:>2}. id={doc_id} score={score:.3f}")
        off += 8

if __name__ == "__main__":
    BASE_URL = "http://127.0.0.1:7700"

    # demo: make sure we have a couple items (optional)
    # ingest_text(BASE_URL, "dj neon")
    # ingest_text(BASE_URL, "neon city")
    # ingest_text(BASE_URL, "dj phantom")
    # ingest_text(BASE_URL, "the neon rider")

    # exact popcount
    search_text(BASE_URL, "neon hypontic", k=5, fuzzy=False)

    # typo-tolerant (Jaccard re-rank on signatures)
    search_text(BASE_URL, "hypnoitc",  k=8, fuzzy=True)

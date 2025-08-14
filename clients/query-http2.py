#!/usr/bin/env python3
import struct
from typing import List
import httpx  # HTTP/2-capable client

# ---------- ahash-lite ----------
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
    return "".join(
        (c if (c.isalnum() or c == " ") else " ")
        for c in s.lower()
    )

def qgrams3(s: str) -> List[bytes]:
    b = normalize(s).encode("ascii", "ignore")
    return [b[i:i+3] for i in range(len(b) - 2)]

# ---------- 4096-bit signature ----------
def grams_to_sig4096(grams: List[bytes]) -> List[int]:
    sig = [0] * 64  # 64 × u64 = 4096 bits
    for g in grams:
        x = ahash64(g)
        for _ in range(4):
            bit = x & 0xFFF  # 12 bits → 0..4095
            word = bit >> 6  # which u64
            bitpos = bit & 63
            sig[word] |= 1 << bitpos
            # xorshift mix
            x ^= (x << 13) & 0xFFFFFFFFFFFFFFFF
            x ^= (x >> 7) & 0xFFFFFFFFFFFFFFFF
            x ^= (x << 17) & 0xFFFFFFFFFFFFFFFF
    return sig

# ---------- wire helpers ----------
FLAG_FUZZY_JACCARD = 1 << 0  # matches server

def make_query_bytes(text: str, k: int = 5, fuzzy: bool = False) -> bytes:
    sig = grams_to_sig4096(qgrams3(text))
    flags = FLAG_FUZZY_JACCARD if fuzzy else 0
    return struct.pack("<HH" + "Q"*64, k, flags, *sig)

def search_text(client: httpx.Client, url: str, text: str, k: int = 5, fuzzy: bool = False):
    q = make_query_bytes(text, k=k, fuzzy=fuzzy)
    r = client.post(url.rstrip("/") + "/search", content=q,
                    headers={"Content-Type": "application/octet-stream"})
    data = r.content
    if len(data) < 4:
        print("[search] bad response")
        return
    (hit_count,) = struct.unpack_from("<I", data, 0)
    print(f"[search] '{text}' k={k} fuzzy={int(fuzzy)} -> {hit_count} hits")
    off = 4
    for i in range(min(hit_count, 10)):  # only first 10
        doc_id, score = struct.unpack_from("<If", data, off)
        print(f"  {i+1:>2}. id={doc_id} score={score:.3f}")
        off += 8

if __name__ == "__main__":
    BASE_URL = "https://127.0.0.1:7700"

    # persistent HTTP/2 TLS client
    with httpx.Client(http2=True, verify=False, timeout=10.0) as client:
        search_text(client, BASE_URL, "neon hypontic", k=5, fuzzy=False)
        search_text(client, BASE_URL, "hypontic", k=8, fuzzy=True)

# strobe

### Compile
```
RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1 -C strip=symbols" cargo build --release
```

### Prepare System (Linux only)
```
# allow more file descriptors in both the testing and the running terminal
ulimit -n 1048576  

# widen ephemeral port range
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"

# lower TIME_WAIT impact
sudo sysctl -w net.ipv4.tcp_fin_timeout=10
sudo sysctl -w net.ipv4.tcp_tw_reuse=1                   

# bigger backlog
sudo sysctl -w net.core.somaxconn=65535
sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65535
```

### Generate Cert for HTTP2
```
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout key.pem -out cert.pem -subj "/CN=localhost"
```

### Generate Test Data (http1 mode only)
```
python3 clients/ingest_mega.py
```

### generate binarized query
```
python3 - <<'PY'       
import struct      
from query_4096 import grams_to_sig4096, qgrams3

TOKEN = "tok47591"  # change if you want a different rare one
k = 5                      
flags = 0  # set to 1 for fuzzy

sig = grams_to_sig4096(qgrams3(TOKEN))                   
payload = struct.pack("<HH" + "Q"*64, k, flags, *sig)
open("q4096.bin", "wb").write(payload)
print(f"Wrote q4096.bin ({len(payload)} bytes) for token '{TOKEN}'")
PY 
```


### Run in HTTP2 mode
```
MODE=h2c \             
BIND=0.0.0.0:7700 \
DATA_DIR=./data \                               
SHARDS=1 \
SHARD_ID=0 \                                                 
WAL_SYNC=coalesce:1048576 \
SWAP_DOCS=4096 \               
SWAP_MS=5 \
./target/release/strobe --cert ./cert.pem --key ./key.pem
```


### Run in HTTP1 mode
```
MODE=h1 \                                                                                                
BIND=0.0.0.0:7700 \
DATA_DIR=./data \
SHARDS=1 \
SHARD_ID=0 \
WAL_SYNC=coalesce:1048576 \
SWAP_DOCS=4096 \
SWAP_MS=5 \
./target/release/strobe --cert ./cert.pem --key ./key.pem
```

### Stress Test
```
h2load \               
  -n 4000000 \
  -c 2000 \
  -m 100 \
  -H 'Content-Type: application/octet-stream' \
  --data=q4096.bin \
  http://127.0.0.1:7700/search
```

or

```
h2load \
  -n 20000000 \
  -c 4000 \
  -m 500 \
  -t 6 \
  -H 'Content-Type: application/octet-stream' \
  --data=q4096.bin \
  http://127.0.0.1:7700/search
``` 


---

##### http3 TBA
```
cargo run --release -- 0.0.0.0:4433 cert.pem key.pem
```



# Test results:
Crawling through 2.000.000 examples at 178k requests/s.
```
h2load \
  -n 20000000 \
  -c 4000 \
  -m 500 \
  -t 6 \
  -H 'Content-Type: application/octet-stream' \
  --data=q4096.bin \
  http://127.0.0.1:7700/search
starting benchmark...
spawning thread #0: 667 total client(s). 3333334 total requests
spawning thread #1: 667 total client(s). 3333334 total requests
spawning thread #2: 667 total client(s). 3333333 total requests
spawning thread #3: 667 total client(s). 3333333 total requests
spawning thread #4: 666 total client(s). 3333333 total requests
spawning thread #5: 666 total client(s). 3333333 total requests
Application protocol: h2c
progress: 9% done
progress: 19% done
progress: 29% done
progress: 39% done
progress: 49% done
progress: 59% done
progress: 69% done
progress: 79% done
progress: 89% done
progress: 99% done

finished in 111.90s, 178729.73 req/s, 4.96MB/s
requests: 20000000 total, 20000000 started, 20000000 done, 20000000 succeeded, 0 failed, 0 errored, 0 timeout
status codes: 20000000 2xx, 0 3xx, 0 4xx, 0 5xx
traffic: 554.57MB (581506517) total, 134.74MB (141286517) headers (space savings 92.48%), 76.29MB (80000000) data
                     min         max         mean         sd        +/- sd
time for request:       66us     108.76s       7.23s       9.72s    86.47%
time for connect:    20.51ms       3.09s       1.14s    963.71ms    69.68%
time to 1st byte:      1.19s     109.57s      13.33s      15.37s    90.43%
req/s           :      44.78      113.81       53.22        7.82    83.18%
```# strobe-engine-client

import http from 'k6/http';
import { Trend, Counter } from 'k6/metrics';

export const options = {
  discardResponseBodies: true,
  noConnectionReuse: false,
  batchPerHost: 16,
  scenarios: {
    warmup: {
      executor: 'constant-vus',
      vus: 500,
      duration: '30s',
    },
    step: {
      executor: 'ramping-arrival-rate',
      startRate: 1000,
      timeUnit: '1s',
      preAllocatedVUs: 2000,
      maxVUs: 4000,
      stages: [
        { target: 2000, duration: '20s' },
        { target: 5000, duration: '20s' },
        { target: 10000, duration: '20s' },
      ],
      gracefulStop: '0s',
      startTime: '30s',
    },
  },
};

  

const latencyMicro = new Trend('latency_microseconds', true);
const reqCount = new Counter('requests_total');

// -------- PRE-BINARIZED QUERY (Little-Endian <HHQQQQ>) --------
// Example: k=5, flags=0, and some dummy sig values
// Replace with real precomputed queries for your workload
const queries = [
  // neon hypnotic
  new Uint8Array([
    0x05, 0x00,  // k=5
    0x00, 0x00,  // flags=0
    // Q0
    0x12, 0x34, 0x56, 0x78, 0xaa, 0xbb, 0xcc, 0xdd,
    // Q1
    0x21, 0x43, 0x65, 0x87, 0xee, 0xdd, 0xcc, 0xbb,
    // Q2
    0x98, 0x76, 0x54, 0x32, 0x10, 0x20, 0x30, 0x40,
    // Q3
    0xde, 0xad, 0xbe, 0xef, 0xfa, 0xce, 0xca, 0xfe,
  ]),
  // hypnoitc fuzzy=1, k=8
  new Uint8Array([
    0x08, 0x00,  // k=8
    0x01, 0x00,  // flags=1 (FUZZY)
    // Q0
    0x11, 0x22, 0x33, 0x44, 0xaa, 0xaa, 0xbb, 0xbb,
    // Q1
    0x55, 0x66, 0x77, 0x88, 0xcc, 0xcc, 0xdd, 0xdd,
    // Q2
    0x99, 0x88, 0x77, 0x66, 0xee, 0xee, 0xff, 0xff,
    // Q3
    0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
  ]),
];

// Pick random query without processing
function getQuery() {
  return queries[(Math.random() * queries.length) | 0];
}

export default function () {
  const query = getQuery();

  const t0 = Date.now(); // ms
  const res = http.post('http://127.0.0.1:7700/search', query, {
    headers: { 'Content-Type': 'application/octet-stream' },
    timeout: '1s',
  });
  const t1 = Date.now();

  // Convert to µs precision
  latencyMicro.add((t1 - t0) * 1000);
  reqCount.add(1);

  if (res.status !== 200) {
    errorCount.add(1);
    // Optional debugging — comment out for real high-RPS runs
    // console.error(`Error: ${res.status} at ${new Date().toISOString()}`);
  }
}

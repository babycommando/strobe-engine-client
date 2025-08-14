import http from "k6/http";

export const options = {
  discardResponseBodies: true,
  noConnectionReuse: false, // keep-alive
  vus: 2000,                // tune until server is full
  duration: "30s",
};

const URL = __ENV.URL || "http://127.0.0.1:7700/search";
const PAYLOAD = (() => {
  const buf = new ArrayBuffer(36);
  const dv = new DataView(buf);
  dv.setUint16(0, 5, true);
  dv.setUint16(2, 0, true);
  // rest zeros
  return buf;
})();

export default function () {
  http.post(URL, PAYLOAD, {
    headers: { "Content-Type": "application/octet-stream" },
    responseType: "none",
    timeout: "5s",
  });
}

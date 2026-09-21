#!/usr/bin/env python3
"""Exercise the compiled Collector against synthetic OTLP and a local Jev stub."""

import http.server
import json
import pathlib
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request


ROOT = pathlib.Path(__file__).resolve().parents[1]


class JevStub(http.server.BaseHTTPRequestHandler):
    calls = 0
    failures = []

    def log_message(self, *_args):
        pass

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path != "/v1/systemone" or self.headers.get("Authorization") != "Bearer smoke-key":
            self.failures.append("invalid inference request")
        if "private-token" in json.dumps(payload):
            self.failures.append("unlisted attribute leaked")
        type(self).calls += 1
        body = json.dumps({"answers": {
            key: {"type": "noul", "noul": 0.1}
            for key in ("diagnostic_value", "business_criticality", "keep")
        }}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def eventually(check, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.05)
    raise AssertionError("Collector did not produce the expected result")


def main():
    stub = http.server.ThreadingHTTPServer(("127.0.0.1", 0), JevStub)
    thread = threading.Thread(target=stub.serve_forever, daemon=True)
    thread.start()
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        otlp_port = reservation.getsockname()[1]
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        telemetry_port = reservation.getsockname()[1]
    try:
        with tempfile.TemporaryDirectory(prefix="jevtraces-smoke-") as directory:
            directory = pathlib.Path(directory)
            config = directory / "collector.yaml"
            config.write_text(f"""receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:{otlp_port}
processors:
  jevtraces:
    api_key: smoke-key
    base_url: http://127.0.0.1:{stub.server_port}
    protected_operations: [checkout]
  batch:
    timeout: 100ms
exporters:
  debug:
    verbosity: detailed
    sampling_initial: 1000
    sampling_thereafter: 1
service:
  telemetry:
    metrics:
      level: normal
      readers:
        - pull:
            exporter:
              prometheus:
                host: 127.0.0.1
                port: {telemetry_port}
    logs:
      encoding: json
  pipelines:
    traces:
      receivers: [otlp]
      processors: [jevtraces, batch]
      exporters: [debug]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
""")
            log = directory / "collector.log"
            with log.open("w") as stream:
                proc = subprocess.Popen(
                    [str(ROOT / "_build/otelcol-jevtraces"), "--config", str(config)],
                    stdout=stream, stderr=subprocess.STDOUT,
                )
                try:
                    def has(text):
                        if proc.poll() is not None:
                            raise AssertionError(log.read_text())
                        return text in log.read_text()

                    eventually(lambda: has("Everything is ready"))

                    def send(index, name="catalog", error=False, slow=False):
                        span = {
                            "traceId": f"{index:032x}", "spanId": f"{index:016x}",
                            "name": name, "kind": 2,
                            "startTimeUnixNano": "100000000000",
                            "endTimeUnixNano": "102000000000" if slow else "100100000000",
                            "status": {"code": 2 if error else 0},
                            "attributes": [{"key": "url.full", "value": {"stringValue": "private-token"}}],
                        }
                        payload = {"resourceSpans": [{
                            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "smoke"}}]},
                            "scopeSpans": [{"scope": {"name": "smoke"}, "spans": [span]}],
                        }]}
                        request = urllib.request.Request(
                            f"http://127.0.0.1:{otlp_port}/v1/traces", data=json.dumps(payload).encode(),
                            headers={"Content-Type": "application/json"}, method="POST",
                        )
                        with urllib.request.urlopen(request, timeout=3) as response:
                            assert response.status == 200

                    send(1)
                    eventually(lambda: has("jevtraces.assessment.state: Str(pending)"))
                    eventually(lambda: JevStub.calls == 1)
                    # A cache hit is visible only on a later span. Wait boundedly for publication.
                    for index in range(2, 12):
                        send(index)
                        time.sleep(0.15)
                        if has("jevtraces.assessment.state: Str(scored)"):
                            break
                    else:
                        raise AssertionError("no cached annotation")
                    send(20, error=True)
                    send(21, slow=True)
                    send(22, name="checkout")
                    for reason in ("error", "slow", "protected_operation"):
                        eventually(lambda reason=reason: has(f"jevtraces.protection.reason: Str({reason})"))
                    assert has("jevtraces.operation.keep_probability: Double(0.1)")
                    metric_payload = {"resourceMetrics": [{
                        "scopeMetrics": [{"scope": {"name": "smoke"}, "metrics": [{
                            "name": "smoke.metric.passthrough",
                            "gauge": {"dataPoints": [{"asInt": "42", "timeUnixNano": "100000000000"}]},
                        }]}],
                    }]}
                    metric_request = urllib.request.Request(
                        f"http://127.0.0.1:{otlp_port}/v1/metrics", data=json.dumps(metric_payload).encode(),
                        headers={"Content-Type": "application/json"}, method="POST",
                    )
                    with urllib.request.urlopen(metric_request, timeout=3) as response:
                        assert response.status == 200
                    eventually(lambda: has("smoke.metric.passthrough"))
                    assert has("Value: 42"), "metric value was not forwarded"
                    assert JevStub.calls == 1, "protected spans or matching operations repeated inference"
                    with urllib.request.urlopen(f"http://127.0.0.1:{telemetry_port}/metrics", timeout=3) as response:
                        exposition = response.read().decode()
                    for metric_name in ("jevtraces_spans_processed", "jevtraces_spans_annotated",
                                        "jevtraces_spans_protected", "jevtraces_inference_requests"):
                        assert metric_name in exposition, f"missing Prometheus counter: {metric_name}"
                    def counter(name):
                        return sum(float(line.split()[1]) for line in exposition.splitlines()
                                   if line and not line.startswith("#")
                                   and line.split()[0].split("{", 1)[0] == name)
                    assert counter("jevtraces_spans_processed") >= 5
                    assert counter("jevtraces_spans_annotated") >= 1
                    assert counter("jevtraces_spans_protected") == 3
                    assert counter("jevtraces_inference_requests") == 1
                    assert not JevStub.failures, JevStub.failures
                    print("PASS: OTLP ingestion, pending/scored annotation, low-score preservation, protection, metric passthrough, Prometheus counters, and one cached inference request")
                except Exception:
                    print(log.read_text())
                    raise
                finally:
                    if proc.poll() is None:
                        proc.send_signal(signal.SIGTERM)
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait()
    finally:
        stub.shutdown()
        stub.server_close()
        thread.join(timeout=2)


if __name__ == "__main__":
    main()

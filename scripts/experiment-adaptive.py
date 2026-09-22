#!/usr/bin/env python3
"""Local experiment against the shipped policy: mock Jev, real tail sampler, OTLP sinks."""
import gzip
import hashlib
import http.server
import json
import pathlib
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
LOCK = threading.Lock()
RECORDS = {"archive": {}, "sampled": {}}
CALLS = 0
VALUE = 0.05
OUTAGE = False

class Server(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_POST(self):
        global CALLS
        raw = self.rfile.read(int(self.headers["Content-Length"]))
        if self.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        payload = json.loads(raw)
        if self.path == "/v1/systemone":
            with LOCK:
                CALLS += 1
                value, outage = VALUE, OUTAGE
            if outage:
                self.send_response(503)
                self.end_headers()
                return
            body = {"answers": {key: {"type": "noul", "noul": value}
                    for key in ("diagnostic_value", "business_criticality", "keep")}}
        else:
            branch = self.path.split('/')[1]
            with LOCK:
                for rs in payload.get("resourceSpans", []):
                    for ss in rs.get("scopeSpans", []):
                        for span in ss.get("spans", []):
                            RECORDS[branch].setdefault(span["traceId"], {})[span["spanId"]] = span
            body = {}
        encoded = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

def eventually(check, timeout=15):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        if check():
            return
        time.sleep(.05)
    raise AssertionError("timed out waiting for experiment result")

def main():
    global VALUE, OUTAGE
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Server)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    proc = None
    try:
        with tempfile.TemporaryDirectory(prefix="jevtraces-adaptive-") as tmp:
            tmp = pathlib.Path(tmp)
            config = (ROOT / "examples/otelcol/config-adaptive.yaml").read_text()
            config = config.replace("127.0.0.1:4318", f"127.0.0.1:{port}")
            config = config.replace("${env:JEV_API_KEY}", "experiment-key")
            config = config.replace("    mode: annotate", f"    base_url: http://127.0.0.1:{server.server_port}\n    mode: annotate")
            config = config.replace("score_ttl: 15m", "score_ttl: 5s").replace("decision_wait: 10s", "decision_wait: 1s")
            config = config.replace("timeout: 1s", "timeout: 100ms")
            for branch in ("archive", "sampled"):
                config = config.replace(f"debug/{branch}:\n    verbosity: detailed", f"otlphttp/{branch}:\n    endpoint: http://127.0.0.1:{server.server_port}/{branch}\n    encoding: json\n    compression: none")
                config = config.replace(f"[debug/{branch}]", f"[otlphttp/{branch}]")
            (tmp / 'config.yaml').write_text(config)
            with (tmp / 'collector.log').open('w') as output:
                proc = subprocess.Popen([str(ROOT / '_build/otelcol-jevtraces'), '--config', str(tmp / 'config.yaml')], stdout=output, stderr=subprocess.STDOUT)
                def ready():
                    if proc.poll() is not None:
                        raise AssertionError((tmp / 'collector.log').read_text())
                    return 'Everything is ready' in (tmp / 'collector.log').read_text()
                eventually(ready)

                def send(index, name='routine', error=False, slow=False, child_only=False, root_only=False):
                    spans = []
                    for offset in ([2] if child_only else [1] if root_only else [1, 2]):
                        span = {'traceId': hashlib.sha256(str(index).encode()).hexdigest()[:32], 'spanId': f'{index*10+offset:016x}',
                                'name': name, 'kind': 2, 'startTimeUnixNano': '100000000000',
                                'endTimeUnixNano': '102000000000' if slow else '100100000000',
                                'status': {'code': 2 if error and offset == 2 else 0}}
                        if offset == 2:
                            span['parentSpanId'] = f'{index*10+1:016x}'
                        spans.append(span)
                    payload = {'resourceSpans': [{'resource': {'attributes': [{'key':'service.name','value':{'stringValue':'experiment'}}]}, 'scopeSpans': [{'scope':{'name':'experiment'},'spans':spans}]}]}
                    req = urllib.request.Request(f'http://127.0.0.1:{port}/v1/traces', data=json.dumps(payload).encode(), headers={'Content-Type':'application/json'})
                    with urllib.request.urlopen(req,timeout=3) as response:
                        assert response.status == 200

                def count(branch, index):
                    with LOCK:
                        return len(RECORDS[branch].get(hashlib.sha256(str(index).encode()).hexdigest()[:32], {}))

                # Cold operations are retained while their assessment is requested.
                send(1)
                eventually(lambda: count('sampled', 1) == 2)
                eventually(lambda: CALLS >= 1)
                # A warm routine-only population should retain a nonzero subset.
                for index in range(100, 300):
                    send(index)
                send(2, error=True)
                send(3, slow=True)
                send(4, name='POST /checkout')
                send(5, name='x'*513)  # skipped metadata must remain retained
                send(6, name='new-operation')  # pending assessment must remain retained
                send(7, root_only=True)
                send(7, error=True, child_only=True)  # later error protects both spans
                for index in range(2, 8):
                    eventually(lambda index=index: count('sampled',index)==2)
                eventually(lambda: all(count('archive',i)==2 for i in range(100,300)))
                time.sleep(2)  # all one-second sampling decisions must have completed
                kept = [i for i in range(100,300) if count('sampled',i)]
                assert 0 < len(kept) < 200, f'baseline did not sample a subset: {len(kept)}'
                assert all(count('sampled',i)==2 for i in kept), 'partial trace exported'
                # Late spans obey the remembered decision (including a dropped trace).
                dropped = next(i for i in range(100,300) if i not in kept)
                send(dropped, child_only=True)
                time.sleep(1.2)
                assert count('sampled',dropped)==0, 'late span escaped cached drop decision'
                # A changed assessment after expiry changes subsequent sampling behavior.
                with LOCK:
                    VALUE = .9
                    previous_calls = CALLS
                time.sleep(5.1)
                send(400)
                eventually(lambda: CALLS > previous_calls)
                eventually(lambda: count('sampled',400)==2)
                for index in range(401,411):
                    send(index)
                eventually(lambda: all(count('sampled',i)==2 for i in range(401,411)))
                # Confirm retention came from refreshed scores, not pending fallback.
                with LOCK:
                    for index in range(401,411):
                        tid = hashlib.sha256(str(index).encode()).hexdigest()[:32]
                        for span in RECORDS['sampled'][tid].values():
                            attrs = {a['key']: a['value'] for a in span['attributes']}
                            assert attrs['jevtraces.assessment.state']['stringValue'] == 'scored'
                            assert attrs['jevtraces.operation.keep_probability']['doubleValue'] == .9
                    for trace in RECORDS['archive'].values():
                        for span in trace.values():
                            assert not any(a['key'].startswith('jevtraces.') for a in span.get('attributes', [])), 'archive mutated'
                # Provider outage for uncached metadata conservatively retains input.
                with LOCK:
                    OUTAGE = True
                send(500,name='outage-operation')
                eventually(lambda: count('sampled',500)==2)
                print(json.dumps({'routine_traces':200,'routine_retained':len(kept),
                    'routine_reduction_percent':(200-len(kept))/2,
                    'after_score_refresh_retained':'10/10',
                    'checks':'cold, error, slow, protected, skipped, pending, split trace, late drop, refresh, outage, archive',
                    'inference':'mock; validates pipeline behavior, not model quality'},indent=2))
                proc.send_signal(signal.SIGTERM)
                proc.wait(timeout=10)
    finally:
        if proc is not None and proc.poll() is None:
            proc.kill(); proc.wait()
        server.shutdown(); server.server_close()

if __name__ == '__main__':
    main()

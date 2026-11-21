"""Local tests for the API circuit breaker in unit_lib (fake clock, no network). Run: python3 tests/test_circuit_breaker.py"""
import os
import sys, threading, time, types
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "geotax_compare", "job"))
import unit_lib as L

class Clock:
    def __init__(self): self.t = 0.0; self.lock = threading.Lock()
    def now(self): return self.t
    def sleep(self, s):
        with self.lock: self.t += s
        time.sleep(0.0005)

def make(statuses, **kw):
    clk = Clock(); seq = iter(statuses); lock = threading.Lock(); calls = []
    class S:
        def post(self, *a, **k):
            with lock:
                st = next(seq, 200); calls.append((clk.t, st))
            return types.SimpleNamespace(status_code=st, text="{}")
    b = L.CircuitBreaker(sleep=clk.sleep, now=clk.now, log=lambda m: None, **kw)
    c = L.ApiClient(S(), "x", 5, 5, sleep=clk.sleep, breaker=b)
    return c, b, clk, calls

# 1. single-thread blip recovers via normal retry, breaker never opens
c, b, clk, calls = make([503, 200])
assert c.post("u", {}, c.tax_limiter, "tax_calls")[0] == 200 and b.opens == 0
# 2. outage: 5 consecutive 503 -> opens -> waits >=24s (30s -0.8) -> probe ok -> continues, no InfraError
c, b, clk, calls = make([503] * 5 + [200])
assert c.post("u", {}, c.tax_limiter, "tax_calls")[0] == 200
assert b.opens == 1 and clk.t >= 24, (b.opens, clk.t)
# 3. longer outage: probe fails -> next cool-off step (60s) -> eventually ok
c, b, clk, calls = make([503] * 7 + [200])
assert c.post("u", {}, c.tax_limiter, "tax_calls")[0] == 200
assert b.step == 0 and clk.t >= 24 + 48, clk.t
# 4. budget exhausted -> InfraError (not an endless loop)
c, b, clk, calls = make([503] * 10_000, budget=300)
try: c.post("u", {}, c.tax_limiter, "tax_calls"); raise SystemExit("expected InfraError")
except L.InfraError as e: assert "budget" in str(e), e
# 5. 4xx never trips the breaker; 429 on a probe does not wedge it
c, b, clk, calls = make([400])
assert c.post("u", {}, c.tax_limiter, "tax_calls")[0] == 400 and b.opens == 0
c, b, clk, calls = make([503] * 5 + [429, 200])
assert c.post("u", {}, c.tax_limiter, "tax_calls")[0] == 200 and not b.is_open()
# 6. 40 threads, API down for the first 400 virtual seconds: threads pause instead of hammering, all recover
clk = Clock(); calls = []; lock = threading.Lock()
class S:
    def post(self, *a, **k):
        with lock:
            st = 503 if clk.t < 400 else 200; calls.append((clk.t, st))
        return types.SimpleNamespace(status_code=st, text="{}")
b = L.CircuitBreaker(sleep=clk.sleep, now=clk.now, log=lambda m: None)
c = L.ApiClient(S(), "x", 5, 5, sleep=clk.sleep, breaker=b)
res = []
ts = [threading.Thread(target=lambda: res.append(c.post("u", {}, c.tax_limiter, "tax_calls")[0])) for _ in range(40)]
[t.start() for t in ts]; [t.join(timeout=120) for t in ts]
assert len(res) == 40 and set(res) == {200}, (len(res), set(res))
bad = [x for x in calls if x[1] == 503]
assert len(bad) <= 40 + 12, len(bad)   # first wave of in-flight calls plus a handful of single probes
print("hammering check: failed calls", len(bad), "of", len(calls), "| opens", b.opens, "| virtual outage s", round(clk.t))
# 7. stop event releases waiting threads
stop = threading.Event(); clk = Clock()
b = L.CircuitBreaker(sleep=clk.sleep, now=clk.now, stop=stop, log=lambda m: None)
for _ in range(5): b.before_call(); b.record(False, 503)
stop.set()
try: b.before_call(); raise SystemExit("expected InfraError")
except L.InfraError: pass
# 8. HTTP 500 "INTERNAL_ERROR ... try again after sometime" is an outage (opens the breaker); a plain 500 is not
class S500:
    def __init__(self, body, n): self.body, self.n = body, n
    def post(self, *a, **k):
        self.n -= 1
        return types.SimpleNamespace(status_code=500 if self.n >= 0 else 200, text=self.body)
for body, expect_open in (('{"errors":[{"status":"INTERNAL_ERROR","errorMessage":"Issue with internal services. Please try again after sometime"}]}', 1), ('{"error":"bad record"}', 0)):
    clk = Clock(); b = L.CircuitBreaker(sleep=clk.sleep, now=clk.now, log=lambda m: None)
    c = L.ApiClient(S500(body, 3), "x", 5, 5, sleep=clk.sleep, breaker=b)
    try: out = c.post("u", {}, c.tax_limiter, "tax_calls")[0]
    except L.InfraError: out = "InfraError"
    assert b.opens == expect_open, (body[:30], b.opens)
    if expect_open: assert out == 200 and clk.t >= 24, (out, clk.t)   # paused, then recovered instead of failing the unit
print("all breaker tests passed")

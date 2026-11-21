"""API client, parsing and row assembly for one work unit. Pure Python (no Spark) so it can be tested locally.

Source: the address_book change feed (ADDED/UPDATED/DELETED), or its current table for the first run of a state.
Flow per chunk: geocode_stage (Geocode opt2 + opt3) -> tax_stage (one Geo Tax call per distinct
coordinate, fanned out to both options) -> assemble (one wide row per address).
"""
import copy
import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

GEOCODE_URL = "https://api.cloud.precisely.com/v1/geocode"
TAX_URL = "https://api.cloud.precisely.com/v1/geo-tax/location/batch"
REQUEST_SIZE = 25  # Geocode API maximum per request
TAX_REQUEST_SIZE = 10  # Geo Tax batch API maximum ("Batch size must not exceed 10")

# Option 2 = enterprise data, Option 3 = premium data. Everything else is identical.
GEOCODE_PREFS_BASE = {
    "maxResults": 1, "returnAllInfo": "true", "factoryDescription": {"label": "ggs", "featureSpecific": {}},
    "clientLocale": "", "clientCoordSysName": "", "distance": {"value": 150, "distanceUnit": "METER"},
    "streetOffset": {"value": 7, "distanceUnit": "METER"}, "cornerOffset": {"value": 7, "distanceUnit": "METER"},
    "fallbackToGeographic": "true", "fallbackToPostal": "true", "matchMode": "STANDARD", "originXY": [],
    "customPreferences": {
        "FALLBACK_TO_STREET_CENTROID": "true", "FIND_STREET_CENTROID": "true", "ADDRESS_CASING": "UPPER",
        "DATA_OPTION": "enterprise", "RETURN_ALT_LANG_FIELDS": "true", "RETURN_PARSED_INPUT": "true",
        "FIND_DPV": "false", "VALIDATION_CONFIDENCE": "false", "USE_ADDRESS_POINT_INTERPOLATION": "false",
        "FALLBACK_TO_WORLD": "false", "FIND_WIDE_SEARCH": "false", "RETURN_H3HEX": "true"},
}
GEOTAX_PREFS = {
    "output": {"taxDistrict": "SPD", "salesTaxRateType": "GENERAL",
               "outputCasing": "MIXED",  # omitting it makes the new API return UPPER; only MIXED and UPPER are valid. MIXED = old endpoint's casing
               "taxCrossReferenceKey": "VERTEX", "usePTC": "NO", "userBoundary": "NO"},
    "geocoding": {"defaultBufferWidth": 0, "latLongOffset": "NONE", "squeeze": "NO",
                  "latLongAltFormat": "DecimalSign", "usePointData": "NO"},
    "matching": {"matchMode": "CLOSE", "useGeoTaxAuxiliaryFile": "NO", "useUserAuxiliaryFile": "NO"},
}


def geocode_prefs(data_option):
    prefs = copy.deepcopy(GEOCODE_PREFS_BASE)
    prefs["customPreferences"]["DATA_OPTION"] = data_option
    return prefs


# ---------------------------------------------------------------------------
# Result columns. Stage status: OK, NO_MATCH (API answered, no usable result), API_ERROR (4xx or bad
# response for that record), SKIPPED (tax not run because geocode failed), NOT_RUN (invalid input).
# ---------------------------------------------------------------------------
GEO_COLS = [
    ("status", "string"), ("http_status", "int"), ("score", "double"), ("lon", "double"), ("lat", "double"),
    ("location_type", "string"), ("location_description", "string"), ("match_code", "string"),
    ("loc_code", "string"), ("precision_level", "string"), ("county_fips", "string"), ("h3hex", "string"),
    ("matched_address", "string"), ("error", "string"), ("raw_response", "string"),
]
TAX_COLS = [
    ("status", "string"), ("http_status", "int"), ("confidence", "double"), ("match_level", "string"),
    ("match_code", "string"), ("state_code", "string"), ("county_code", "string"), ("county_name", "string"),
    ("place_code", "string"), ("place_name", "string"), ("spd_districts", "string"), ("total_tax_rate", "double"),
    ("state_tax_rate", "double"), ("county_tax_rate", "double"), ("municipal_tax_rate", "double"),
    ("matched_address", "string"), ("jurisdiction_key", "string"), ("error", "string"), ("raw_response", "string"),
]
KEY_COLS = [
    ("PBKEY", "string"), ("ADMIN1", "string"), ("ADMIN2", "string"), ("ADDRLINE1", "string"), ("CITY", "string"),
    ("POSTALCODE", "string"), ("change_type", "string"), ("release_seq", "bigint"), ("release_version", "string"),
    ("batch_seq", "bigint"), ("unit_id", "int"), ("n_units", "int"), ("run_id", "string"),
    ("processed_at", "timestamp"), ("row_status", "string"),
]
# Columns that identify a record and survive a source DELETE; every other result column is set to NULL.
TOMBSTONE_KEEP = {"PBKEY", "ADMIN1"}
RESULT_COLS = (
    KEY_COLS
    + [(f"geo2_{n}", t) for n, t in GEO_COLS] + [(f"geo3_{n}", t) for n, t in GEO_COLS]
    + [(f"tax2_{n}", t) for n, t in TAX_COLS] + [(f"tax3_{n}", t) for n, t in TAX_COLS]
)
RESULT_NAMES = [n for n, _ in RESULT_COLS]


LEDGER_COLS = [
    ("state", "string"), ("run_mode", "string"), ("sample_size", "int"), ("batch_seq", "bigint"), ("from_seq", "bigint"),
    ("release_version", "string"), ("n_units", "int"), ("unit_id", "int"), ("status", "string"),
    ("rows_expected", "bigint"), ("rows_written", "bigint"), ("pbkey_checksum", "bigint"),
    ("rows_processed_this_run", "bigint"), ("rows_skipped_resume", "bigint"), ("geocode_calls", "bigint"),
    ("tax_calls", "bigint"), ("tax_lookups_needed", "bigint"), ("tax_locations_sent", "bigint"), ("retries", "bigint"),
    ("geo_throttles", "int"), ("tax_throttles", "int"), ("api_seconds", "double"), ("wall_seconds", "double"),
    ("run_id", "string"), ("started_at", "timestamp"), ("finished_at", "timestamp"),
]
LEDGER_COMMENT = "Commit marker per work unit: a COMPLETE row is written only after every address of the unit is in the results table."


def ddl_columns(cols):
    return ", ".join(f"{n} {t.upper()}" for n, t in cols)


# One row per record the gate loaded for a run: BASELINE / ADDED / UPDATED rows are sent to the APIs, DELETED rows null the record.
WORK_COLS = [
    ("PBKEY", "string"), ("ADMIN1", "string"), ("ADMIN2", "string"), ("ADDRLINE1", "string"), ("CITY", "string"),
    ("POSTALCODE", "string"), ("POSTALCODEEXT", "string"), ("change_type", "string"), ("release_seq", "bigint"),
    ("release_version", "string"), ("batch_seq", "bigint"), ("from_seq", "bigint"), ("run_id", "string"),
    ("loaded_at", "timestamp"), ("is_valid", "boolean"), ("invalid_reason", "string"),
]
CONTROL_COLS = [
    ("state", "string"), ("run_mode", "string"), ("sample_size", "int"), ("kind", "string"), ("from_seq", "bigint"),
    ("batch_seq", "bigint"), ("release_version", "string"), ("status", "string"), ("run_id", "string"),
    ("rows_loaded", "bigint"), ("deletes_loaded", "bigint"), ("loaded_at", "timestamp"), ("completed_at", "timestamp"),
]
PREFLIGHT_COLS = [
    ("run_id", "string"), ("state", "string"), ("batch_seq", "bigint"), ("check_name", "string"), ("passed", "boolean"),
    ("detail", "string"), ("checked_at", "timestamp"),
]
RECONCILE_COLS = [
    ("state", "string"), ("run_mode", "string"), ("batch_seq", "bigint"), ("n_units", "int"), ("rows_expected", "bigint"),
    ("staged_rows", "bigint"), ("distinct_staged_rows", "bigint"), ("missing", "bigint"), ("rows_ok", "bigint"),
    ("rows_geocode_failed", "bigint"), ("rows_tax_failed", "bigint"), ("rows_invalid_input", "bigint"),
    ("units_complete", "int"), ("ledger_rows_written", "bigint"), ("deletes_expected", "bigint"),
    ("deletes_applied", "bigint"), ("merged_rows", "bigint"), ("passed", "boolean"), ("detail", "string"),
    ("checked_at", "timestamp"),
]


def table_names(catalog, run_mode):
    """All table names for a run mode. SAMPLE runs use *_sample copies of the work, staging and results tables."""
    sfx = "_sample" if run_mode == "SAMPLE" else ""
    bronze, silver, ops = (f"{catalog}.geotax_compare_{s}" for s in ("bronze", "silver", "ops"))
    return {
        "bronze": bronze, "silver": silver, "ops": ops, "gold": f"{catalog}.geotax_compare_gold",
        "work": f"{bronze}.work_items{sfx}", "staging": f"{silver}.results_staging{sfx}", "results": f"{silver}.results{sfx}",
        "control": f"{bronze}.run_control", "ledger": f"{bronze}.unit_ledger", "preflight": f"{ops}.preflight_log",
        "reconcile_log": f"{ops}.reconcile_log",
    }


def ensure_tables(spark, catalog):
    """Idempotent: create the schemas and every table the job writes."""
    t = table_names(catalog, "FULL")
    for schema in ("bronze", "silver", "ops", "gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {t[schema]}")
    for mode in ("FULL", "SAMPLE"):
        n = table_names(catalog, mode)
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {n['work']} ({ddl_columns(WORK_COLS)})
            CLUSTER BY (ADMIN1, batch_seq)
            COMMENT 'Records loaded per run from the address_book source: BASELINE (first run of a state) or ADDED/UPDATED/DELETED (delta).'""")
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {n['staging']} ({ddl_columns(RESULT_COLS)})
            CLUSTER BY (ADMIN1, batch_seq, unit_id)
            COMMENT 'Append-only unit output per run. Reconcile merges a verified run into results.'""")
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {n['results']} ({ddl_columns(RESULT_COLS)})
            CLUSTER BY (ADMIN1, PBKEY)
            COMMENT 'Current state: one row per PBKEY with the latest Geocode opt2/opt3 + Geo Tax result. row_status DELETED = removed in the source, all other columns NULL.'""")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {t['control']} ({ddl_columns(CONTROL_COLS)}) COMMENT 'One row per (state, run mode, batch): LOADED then DONE. The DONE batch_seq is the state watermark: the next run only processes source changes after it.'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {t['ledger']} ({ddl_columns(LEDGER_COLS)}) COMMENT '{LEDGER_COMMENT}'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {t['preflight']} ({ddl_columns(PREFLIGHT_COLS)})")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {t['reconcile_log']} ({ddl_columns(RECONCILE_COLS)})")


class BaselineBlocked(RuntimeError):
    """A state with no finished run would be processed in full (the expensive first run) without being allowed to."""


def decide_action(watermark, open_runs, to_seq, run_mode="FULL", allow_baseline=False):
    """What a state's run does: RESUME (an earlier batch was loaded but not finished), SKIP (already at the newest release),
    BASELINE (first run: every current record) or DELTA (changes after the watermark).

    A FULL baseline bills about 4 API calls per address, so it is refused unless `allow_baseline` is set: a wrong catalog or a
    wiped control table must not silently re-process the whole address fabric. SAMPLE runs are always allowed."""
    if open_runs:
        return "RESUME"
    if watermark is not None and watermark >= to_seq:
        return "SKIP"
    if watermark is None:
        if run_mode == "FULL" and not allow_baseline:
            raise BaselineBlocked("no finished run exists for this state, so this would process every record; "
                                  "set the job parameter allow_baseline=true if that is intended")
        return "BASELINE"
    return "DELTA"


def states_needing_work(all_states, control_rows, to_seq, run_mode="FULL", allow_baseline=False):
    """Splits states into (to_run, blocked, up_to_date) from the control rows ({state, run_mode, batch_seq, status}).
    `to_run` keeps the order of `all_states`; `blocked` are never-processed states that need allow_baseline."""
    to_run, blocked, current = [], [], []
    for state in all_states:
        rows = [r for r in control_rows if r["state"] == state and r["run_mode"] == run_mode]
        done = [r["batch_seq"] for r in rows if r["status"] == "DONE"]
        watermark = max(done) if done else None
        open_runs = [r for r in rows if r["status"] == "LOADED" and (watermark is None or r["batch_seq"] > watermark)]
        try:
            action = decide_action(watermark, open_runs, to_seq, run_mode, allow_baseline)
        except BaselineBlocked:
            blocked.append(state)
            continue
        (current if action == "SKIP" else to_run).append(state)
    return to_run, blocked, current


class InfraError(RuntimeError):
    """Retries exhausted on a transient failure: the unit fails and is re-driven by a re-run."""


class AdaptiveLimiter:
    """Caps in-flight calls; halves the cap on a 429 and creeps back up after sustained success.

    `rate` (calls per second, optional) also spaces call starts evenly: the Geo Tax API enforces a per-second rate limit,
    so an in-flight cap alone cannot keep a fast API under it. A 429 halves the rate; sustained success restores it."""

    def __init__(self, maximum, rate=None):
        self.max = self.limit = maximum
        self.rate_max = self.rate = rate
        self.inflight = 0
        self.throttled = 0
        self._ok = 0
        self._next = 0.0
        self._cond = threading.Condition()

    def __enter__(self):
        delay = 0.0
        with self._cond:
            while self.inflight >= self.limit:
                self._cond.wait()
            self.inflight += 1
            if self.rate:
                now = time.monotonic()
                start = max(now, self._next)
                self._next = start + 1.0 / self.rate
                delay = start - now
        if delay > 0:
            time.sleep(delay)

    def __exit__(self, *exc):
        with self._cond:
            self.inflight -= 1
            self._cond.notify_all()

    def success(self):
        with self._cond:
            self._ok += 1
            if self._ok >= 50:
                self._ok = 0
                if self.limit < self.max:
                    self.limit += 1
                if self.rate and self.rate < self.rate_max:
                    self.rate = min(self.rate_max, self.rate * 1.25)
                self._cond.notify_all()

    def throttle(self):
        with self._cond:
            self.throttled += 1
            self.limit = max(1, self.limit // 2)
            if self.rate:
                self.rate = max(0.5, self.rate / 2)
            self._ok = 0


class CircuitBreaker:
    """Process-wide pause for an API outage (502/503/504/connection errors).

    Closed: calls flow. After `threshold` consecutive infra failures (any thread) it opens: every thread waits out a
    cool-off (30s, 60s, 120s, 180s, then 300s, +-20% jitter). Then exactly one thread is let through as a probe; success
    closes the breaker, failure re-opens it at the next step. If one continuous outage lasts longer than `budget`
    seconds, before_call raises InfraError so the unit fails and the job-level retry / a re-run resumes it."""

    STEPS = (30, 60, 120, 180, 300)

    def __init__(self, threshold=5, budget=1200, sleep=time.sleep, now=time.monotonic, stop=None, log=print):
        self.threshold, self.budget = threshold, budget
        self._sleep, self._now, self._stop, self._log = sleep, now, stop, log
        self._lock = threading.Lock()
        self.fails = 0
        self.step = 0
        self.state = "closed"  # closed | open | probing
        self.reopen_at = 0.0
        self.outage_start = None
        self.opens = 0
        self.waited = 0.0  # seconds threads spent waiting on the breaker (summed over threads)

    def is_open(self):
        return self.state != "closed"

    def before_call(self):
        """Blocks while the breaker is open. Returns when the caller may call (closed, or it is the single probe)."""
        while True:
            if self._stop is not None and self._stop.is_set():
                raise InfraError("stopped while the API circuit breaker was open")
            with self._lock:
                if self.state == "closed":
                    return
                now = self._now()
                if now - self.outage_start > self.budget:
                    raise InfraError(f"API outage lasted longer than the {self.budget}s budget")
                if self.state == "open" and now >= self.reopen_at:
                    self.state = "probing"
                    self._log("circuit breaker: probing the API")
                    return
            self._sleep(1.0)
            self.waited += 1.0

    def record(self, ok, status=None):
        with self._lock:
            if ok:
                if self.state != "closed":
                    self._log(f"circuit breaker CLOSED after {self._now() - self.outage_start:.0f}s outage")
                    self.state, self.step, self.outage_start = "closed", 0, None
                self.fails = 0
                return
            self.fails += 1
            if self.state == "open":
                return  # a call that was already in flight when the breaker opened: nothing new to learn
            if self.state == "probing" or self.fails >= self.threshold:
                self._open(status)

    def trip(self, status=None):
        """Open the breaker now: a thread is out of attempts, which is an outage even if fewer than `threshold` threads saw it."""
        with self._lock:
            if self.state == "closed":
                self._open(status)

    def _open(self, status):  # caller holds the lock
        if self.state == "closed":
            self.outage_start = self._now()
            self.opens += 1
        delay = self.STEPS[min(self.step, len(self.STEPS) - 1)] * (0.8 + 0.4 * random.random())
        self.reopen_at = self._now() + delay
        self.state, self.step = "open", self.step + 1
        self._log(f"circuit breaker OPEN (http {status}), cool-off {delay:.0f}s, step {self.step}")


def is_outage(status, text):
    """502/503/504 and connection errors are outages. So is a 500 that says to try again later (the APIs answer
    'INTERNAL_ERROR ... Please try again after sometime' when a backend is down); any other 500 is not."""
    return status in (0, 502, 503, 504) or (status == 500 and "try again" in (text or "").lower())


class ApiClient:
    def __init__(self, session, auth_header, geo_concurrency, tax_concurrency, purchase_amount=100.0,
                 retries=3, sleep=time.sleep, stop=None, breaker=None, tax_rate=None):
        self.session, self.purchase_amount, self.retries, self._sleep = session, purchase_amount, retries, sleep
        self.breaker = breaker or CircuitBreaker(sleep=sleep, stop=stop)
        self._headers = {"Authorization": auth_header, "Content-Type": "application/json"}
        self.geo_limiter = AdaptiveLimiter(geo_concurrency)
        self.tax_limiter = AdaptiveLimiter(tax_concurrency, rate=tax_rate)
        self.stats = {"geocode_calls": 0, "tax_calls": 0, "retries": 0, "api_seconds": 0.0}
        self._lock = threading.Lock()

    def _bump(self, key, n=1):
        with self._lock:
            self.stats[key] += n

    def post(self, url, body, limiter, kind):
        last = (0, "no attempt")
        attempt = 0
        while attempt < self.retries:
            self.breaker.before_call()  # waits out an API outage; time spent there does not use up attempts
            t0 = time.perf_counter()
            with limiter:
                try:
                    r = self.session.post(url, headers=self._headers, json=body, timeout=(10, 120))
                    status, text = r.status_code, r.text
                except Exception as e:  # network errors: never include headers in the message
                    status, text = 0, f"{type(e).__name__}: {str(e)[:200]}"
            self._bump(kind)
            self._bump("api_seconds", time.perf_counter() - t0)
            if status == 429:
                limiter.throttle()
            infra = is_outage(status, text)
            self.breaker.record(not infra, status)  # 429 and other 500s mean the API answered: not an outage
            if status in (0, 429, 500, 502, 503, 504):
                last = (status, text[:300])
                self._bump("retries")
                if infra and not self.breaker.is_open() and attempt + 1 >= self.retries:
                    self.breaker.trip(status)  # last attempt failed on an infra error: pause instead of giving up
                if infra and self.breaker.is_open():
                    attempt = 0  # the breaker owns the wait (and its budget); start fresh once the API answers again
                    continue
                self._sleep(min(30, 2 ** attempt) + random.random())
                attempt += 1
                continue
            if status == 200:
                limiter.success()
            return status, text
        raise InfraError(f"retries exhausted: http {last[0]} {last[1]}")


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def _empty(cols):
    return {n: None for n, _ in cols}


def api_error(cols, status, message):
    d = _empty(cols)
    d.update(status="API_ERROR", http_status=status, error=str(message)[:500])
    return d


def parse_geocode(item):
    d = _empty(GEO_COLS)
    d["http_status"] = 200
    status = item.get("status")
    res = (item.get("results") or [None])[0] or {}
    loc = res.get("location") or {}
    expl = loc.get("explanation") or {}
    coords = ((loc.get("feature") or {}).get("geometry") or {}).get("coordinates") or []
    cf = res.get("customFields") or {}
    d.update(
        score=res.get("score"),
        lon=coords[0] if len(coords) >= 2 else None, lat=coords[1] if len(coords) >= 2 else None,
        location_type=expl.get("type"), location_description=expl.get("description"),
        match_code=cf.get("MATCH_CODE"), loc_code=cf.get("LOC_CODE"), precision_level=cf.get("PRECISION_LEVEL"),
        county_fips=cf.get("COUNTY_FIPS"), h3hex=cf.get("H3HEX"),
        matched_address=(res.get("address") or {}).get("formattedAddress"),
    )
    if status == "OK" and d["lon"] is not None and d["lat"] is not None:
        d["status"] = "OK"
    else:
        d["status"] = "NO_MATCH"
        d["error"] = f"GEOCODE_STATUS_{status}" if status != "OK" else "NO_COORDINATES"
        d["raw_response"] = json.dumps(item)
    return d


def parse_tax(item):
    d = _empty(TAX_COLS)
    d["http_status"] = 200
    if item is None:
        d.update(status="NO_MATCH", error="no taxResponse returned for record")
        return d
    j = item.get("jurisdiction") or {}
    st = item.get("salesTax") or {}
    census = item.get("census") or {}
    spds = sorted(s.get("districtNumber") or "" for s in (j.get("spds") or []))
    d.update(
        confidence=item.get("confidence"), match_level=census.get("matchLevel"), match_code=census.get("matchCode"),
        state_code=(j.get("state") or {}).get("code"), county_code=(j.get("county") or {}).get("code"),
        county_name=(j.get("county") or {}).get("name"), place_code=(j.get("place") or {}).get("code"),
        place_name=(j.get("place") or {}).get("name"), spd_districts=",".join(spds),
        total_tax_rate=st.get("totalTaxRate"), state_tax_rate=st.get("stateTaxRate"),
        county_tax_rate=st.get("countyTaxRate"), municipal_tax_rate=st.get("municipalTaxRate"),
        matched_address=(item.get("matchedAddress") or {}).get("formattedAddress"),
    )
    d["jurisdiction_key"] = "|".join([d["state_code"] or "", d["county_code"] or "", d["place_code"] or "", d["spd_districts"]])
    if d["total_tax_rate"] is not None and d["state_code"] is not None:
        d["status"] = "OK"
    else:
        d["status"] = "NO_MATCH"
        d["error"] = "NO_TAX_RATE_OR_JURISDICTION"
        d["raw_response"] = json.dumps(item)
    return d


# ---------------------------------------------------------------------------
# API calls. A non-retryable 4xx on a multi-record request is re-sent one record at a time, so
# one bad record cannot poison its neighbours.
# ---------------------------------------------------------------------------
def geocode_call(client, prefs, addresses):
    status, text = client.post(GEOCODE_URL, {"preferences": prefs, "addresses": addresses}, client.geo_limiter, "geocode_calls")
    if status == 200:
        try:
            responses = json.loads(text).get("responses")
        except ValueError:
            responses = None
        if isinstance(responses, list) and len(responses) == len(addresses):
            return [parse_geocode(r) for r in responses]
        return [api_error(GEO_COLS, status, f"unexpected response shape: {text[:300]}") for _ in addresses]
    if 400 <= status < 500 and len(addresses) > 1:
        return [r for a in addresses for r in geocode_call(client, prefs, [a])]
    return [api_error(GEO_COLS, status, text) for _ in addresses]


def tax_call(client, coords):
    """coords: [(lon, lat)] as strings. The batch API has no record ids: responses[i] answers locations[i]."""
    locations = [{"longitude": float(lon), "latitude": float(lat)} for lon, lat in coords]
    status, text = client.post(TAX_URL, {"preferences": GEOTAX_PREFS, "locations": locations}, client.tax_limiter, "tax_calls")
    if status == 200:
        try:
            responses = json.loads(text).get("responses")
        except ValueError:
            return [api_error(TAX_COLS, status, f"invalid JSON: {text[:300]}") for _ in coords]
        if not isinstance(responses, list) or len(responses) != len(coords):
            return [api_error(TAX_COLS, status, f"unexpected response shape: {text[:300]}") for _ in coords]
        return [parse_tax_response(r) for r in responses]
    if 400 <= status < 500 and len(coords) > 1:
        return [r for c in coords for r in tax_call(client, [c])]
    return [api_error(TAX_COLS, status, text) for _ in coords]


def parse_tax_response(item):
    """One entry of `responses`: {"status": "OK", "result": {...}}. Anything else is a per-record failure."""
    if not isinstance(item, dict) or item.get("status") != "OK" or not item.get("result"):
        d = _empty(TAX_COLS)
        d.update(status="NO_MATCH", http_status=200, error=f"TAX_STATUS_{(item or {}).get('status')}", raw_response=json.dumps(item))
        return d
    return parse_tax(item["result"])


def _batches(items, size=REQUEST_SIZE):
    return [items[i:i + size] for i in range(0, len(items), size)]


def address_body(row):
    return {"addressLines": [row["ADDRLINE1"]], "city": row["CITY"], "postalCode": row["POSTALCODE"],
            "admin1": row["ADMIN1"], "country": "usa"}


def geocode_stage(client, pool, rows, prefs2, prefs3):
    """rows: valid address dicts. Returns (geo2, geo3): lists aligned with rows."""
    bodies = [address_body(r) for r in rows]
    jobs = [(prefs, b) for prefs in (prefs2, prefs3) for b in _batches(bodies)]
    results = list(pool.map(lambda j: geocode_call(client, j[0], j[1]), jobs))
    n = len(_batches(bodies))
    flat = [[x for part in results[k * n:(k + 1) * n] for x in part] for k in range(2)]
    return flat[0], flat[1]


def coord_key(g):
    return (str(g["lon"]), str(g["lat"]))


def tax_stage(client, pool, geo2, geo3):
    """One Geo Tax call per distinct coordinate across both options. Returns ({coord: tax}, lookups_needed)."""
    needed = [coord_key(g) for g in geo2 + geo3 if g["status"] == "OK"]
    distinct = list(dict.fromkeys(needed))
    parts = list(pool.map(lambda b: tax_call(client, b), _batches(distinct, TAX_REQUEST_SIZE)))
    flat = [x for part in parts for x in part]
    return dict(zip(distinct, flat)), len(needed)


def _prefixed(prefix, d):
    return {f"{prefix}_{k}": v for k, v in d.items()}


def _snapshot(r):
    return {k: r.get(k) for k in ("PBKEY", "ADMIN1", "ADMIN2", "ADDRLINE1", "CITY", "POSTALCODE", "change_type",
                                  "release_seq", "release_version")}


def assemble(valid_rows, geo2, geo3, taxmap, meta, invalid_rows=()):
    """Wide result rows. Each input row carries the address snapshot and release fields of the source record.
    meta: batch_seq, unit_id, n_units, run_id, processed_at."""
    out = []
    for r in invalid_rows:
        d = {c: None for c in RESULT_NAMES}
        d.update(_snapshot(r), row_status="INVALID_INPUT", **meta)
        for p in ("geo2", "geo3", "tax2", "tax3"):
            d[f"{p}_status"] = "NOT_RUN"
        d["geo2_error"] = d["geo3_error"] = r.get("invalid_reason")
        out.append(d)
    for r, g2, g3 in zip(valid_rows, geo2, geo3):
        d = {**_snapshot(r), **meta}
        d.update(_prefixed("geo2", g2))
        d.update(_prefixed("geo3", g3))
        for p, g in (("tax2", g2), ("tax3", g3)):
            if g["status"] == "OK":
                d.update(_prefixed(p, taxmap[coord_key(g)]))
            else:
                d.update(_prefixed(p, {**_empty(TAX_COLS), "status": "SKIPPED"}))
        if g2["status"] == "OK" and g3["status"] == "OK" and d["tax2_status"] == "OK" and d["tax3_status"] == "OK":
            d["row_status"] = "OK"
        elif g2["status"] != "OK" or g3["status"] != "OK":
            d["row_status"] = "GEOCODE_FAILED"
        else:
            d["row_status"] = "TAX_FAILED"
        out.append(d)
    return out

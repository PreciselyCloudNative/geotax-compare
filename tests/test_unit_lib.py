"""Local tests for result-row assembly and table definitions in unit_lib (no Spark, no network). Run: python3 tests/test_unit_lib.py"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "geotax_compare", "job"))
import unit_lib as L  # noqa: E402

META = dict(batch_seq=2, unit_id=0, n_units=1, run_id="r1", processed_at=None)
REC = dict(PBKEY="P1", ADMIN1="AZ", ADMIN2="Maricopa", ADDRLINE1="1 MAIN ST", CITY="PHOENIX", POSTALCODE="85001",
           change_type="UPDATED", release_seq=2, release_version="2026.11.1")


def geo(lon, lat, status="OK"):
    d = {n: None for n, _ in L.GEO_COLS}
    d.update(status=status, lon=lon, lat=lat)
    return d


def tax(rate):
    d = {n: None for n, _ in L.TAX_COLS}
    d.update(status="OK", total_tax_rate=rate)
    return d


def test_ok_row_carries_snapshot_and_release():
    g2, g3 = geo(1.0, 2.0), geo(1.0, 2.1)
    taxmap = {L.coord_key(g2): tax(0.08), L.coord_key(g3): tax(0.09)}
    (row,) = L.assemble([REC], [g2], [g3], taxmap, META)
    assert row["row_status"] == "OK" and row["tax2_total_tax_rate"] == 0.08 and row["tax3_total_tax_rate"] == 0.09
    assert row["ADDRLINE1"] == "1 MAIN ST" and row["change_type"] == "UPDATED" and row["release_seq"] == 2 and row["batch_seq"] == 2
    assert set(row) == set(L.RESULT_NAMES)


def test_geocode_failure_skips_tax():
    g2, g3 = geo(None, None, "NO_MATCH"), geo(1.0, 2.0)
    (row,) = L.assemble([REC], [g2], [g3], {L.coord_key(g3): tax(0.09)}, META)
    assert row["row_status"] == "GEOCODE_FAILED" and row["tax2_status"] == "SKIPPED" and row["tax3_status"] == "OK"


def test_invalid_input_row():
    bad = dict(REC, invalid_reason="ADDRLINE1_MISSING")
    (row,) = L.assemble([], [], [], {}, META, invalid_rows=[bad])
    assert row["row_status"] == "INVALID_INPUT" and row["geo2_status"] == "NOT_RUN" and row["geo2_error"] == "ADDRLINE1_MISSING"


def test_tombstone_columns():
    names = [n for n, _ in L.RESULT_COLS]
    assert L.TOMBSTONE_KEEP <= set(names)
    nulled = [n for n in names if n not in L.TOMBSTONE_KEEP | {"change_type", "release_seq", "release_version", "batch_seq", "processed_at", "row_status"}]
    assert "ADDRLINE1" in nulled and "geo2_lat" in nulled and "tax3_total_tax_rate" in nulled and "PBKEY" not in nulled


def test_table_names_sample_vs_full():
    assert L.table_names("c", "FULL")["results"] == "c.geotax_compare_silver.results"
    assert L.table_names("c", "SAMPLE")["results"] == "c.geotax_compare_silver.results_sample"
    assert L.table_names("c", "SAMPLE")["control"] == L.table_names("c", "FULL")["control"]
    n = L.table_names("c", "FULL")
    assert n["preflight"] == "c.geotax_compare_ops.preflight_log" and n["reconcile_log"] == "c.geotax_compare_ops.reconcile_log"
    assert n["control"].startswith("c.geotax_compare_bronze.")



def test_decide_action():
    open_run = [{"batch_seq": 3, "status": "LOADED"}]
    assert L.decide_action(1, open_run, 3) == "RESUME"           # an unfinished batch is finished first
    assert L.decide_action(3, [], 3) == "SKIP"                   # already at the newest release
    assert L.decide_action(4, [], 3) == "SKIP"
    assert L.decide_action(1, [], 3) == "DELTA"                  # behind: only the changes
    assert L.decide_action(None, [], 3, "FULL", allow_baseline=True) == "BASELINE"
    assert L.decide_action(None, [], 3, "SAMPLE") == "BASELINE"  # sample runs never need the flag
    assert L.decide_action(None, open_run, 3) == "RESUME"        # resuming a baseline needs no flag either


def test_full_baseline_is_refused_without_the_flag():
    try:
        L.decide_action(None, [], 3, "FULL")
    except L.BaselineBlocked as e:
        assert "allow_baseline" in str(e)
    else:
        raise AssertionError("a FULL baseline must be refused unless allow_baseline is set")


def test_states_needing_work():
    rows = [
        {"state": "AZ", "run_mode": "FULL", "batch_seq": 1, "status": "DONE"},   # behind -> delta
        {"state": "TX", "run_mode": "FULL", "batch_seq": 3, "status": "DONE"},   # current -> skipped
        {"state": "VA", "run_mode": "FULL", "batch_seq": 1, "status": "DONE"},
        {"state": "VA", "run_mode": "FULL", "batch_seq": 3, "status": "LOADED"},  # unfinished -> resumed
        {"state": "NY", "run_mode": "SAMPLE", "batch_seq": 3, "status": "DONE"},  # another run mode: ignored
    ]
    run, blocked, current = L.states_needing_work(["AZ", "NY", "TX", "VA", "WY"], rows, 3)
    assert run == ["AZ", "VA"] and current == ["TX"] and blocked == ["NY", "WY"]  # never processed in FULL mode
    run, blocked, current = L.states_needing_work(["AZ", "NY", "TX", "VA", "WY"], rows, 3, allow_baseline=True)
    assert run == ["AZ", "NY", "VA", "WY"] and blocked == [] and current == ["TX"]



def test_tax_call_batch_shape_and_positional_mapping():
    sent = {}

    class Resp:
        status_code = 200
        text = '{"responses":[{"status":"OK","result":{"confidence":100,"jurisdiction":{"state":{"code":"04"},"county":{"code":"015","name":"Mohave"},"place":{"code":"1","name":"X"}},"salesTax":{"totalTaxRate":0.056}}},{"status":"ERROR"}]}'

    class Session:
        def post(self, url, headers, json, timeout):
            sent.update(url=url, body=json)
            return Resp()

    client = L.ApiClient(Session(), "Apikey x", 1, 1)
    ok, bad = L.tax_call(client, [("-114.0", "35.9"), ("-111.1", "32.3")])
    assert sent["url"].endswith("/v1/geo-tax/location/batch")
    assert sent["body"]["locations"] == [{"longitude": -114.0, "latitude": 35.9}, {"longitude": -111.1, "latitude": 32.3}]
    assert sent["body"]["preferences"]["output"]["outputCasing"] == "MIXED"
    assert ok["status"] == "OK" and ok["total_tax_rate"] == 0.056 and ok["county_name"] == "Mohave"
    assert bad["status"] == "NO_MATCH" and bad["error"] == "TAX_STATUS_ERROR"


def test_tax_batch_size_is_ten():
    assert L.TAX_REQUEST_SIZE == 10 and len(L._batches(list(range(25)), L.TAX_REQUEST_SIZE)) == 3


def test_rate_limiter_spaces_calls_and_halves_on_429():
    lim = L.AdaptiveLimiter(4, rate=20.0)
    t0 = time.monotonic()
    for _ in range(5):
        with lim:
            pass
    assert time.monotonic() - t0 >= 0.19  # 5 calls at 20/s: starts 50ms apart
    lim.throttle()
    assert lim.rate == 10.0 and lim.limit == 2


if __name__ == "__main__":
    tests = [(n, f) for n, f in list(globals().items()) if n.startswith("test_") and callable(f)]
    for n, f in tests:
        f()
    print(f"all {len(tests)} unit_lib tests passed")

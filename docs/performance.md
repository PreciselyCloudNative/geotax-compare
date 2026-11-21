# Performance and run times

Measured on the first complete run: **194.5M addresses, 54 states and territories, release 2026.10.5** (November 2026), and on the first incremental run that followed. Times are wall clock on serverless compute, four units in parallel.

## First run (baseline)

| | Addresses | Processing time | Geo Tax setting |
|---|---|---|---|
| 50 states and territories (release 2026.10.3) | 133.1M | about 14 hours in total (0.5 to 1.1 hours for a 5 to 7M state) | previous endpoint, 25 locations per call, no rate limit |
| NY (2026.10.4) | 10.7M | 8.8 hours | batch endpoint, 36 calls/s |
| FL (2026.10.4) | 14.9M | 15.7 hours | batch endpoint, 36 calls/s |
| TX (2026.10.5) | 17.6M | 14.2 hours | batch endpoint, 48 calls/s |
| CA (2026.10.5) | 18.2M | 14.8 hours | batch endpoint, 48 calls/s |
| **Total** | **194.5M** | **about 66 hours** of processing | |

The first run took 10 days of calendar time (11 to 21 November) because it was paused and restarted while the API capacity was being agreed, and because the Geo Tax endpoint changed in the middle (see below). Those pauses cost no work: every restart resumed unfinished states and units.

API volume of the first run: 15.5M Geocode requests (0 throttled) and 15.5M Geo Tax calls carrying 269M locations, with 370 throttled (0.002%) and 16,207 retries (0.05%), almost all of them (15,674) in the first 50 states.

**What a re-run of the whole first run would cost today.** One address needs about 0.08 Geocode requests and about 0.134 Geo Tax calls (1.34 locations after de-duplication, 10 per call). At 48 Geo Tax calls per second that is about **350 addresses per second, 194.5M addresses in about 150 hours (6.3 days)**, with about 15.6M Geocode requests and 26M Geo Tax calls. This is why a baseline is refused unless `allow_baseline=true`.

## Why the speed changed during the first run

Geo Tax is the bottleneck. A unit's time is its Geo Tax calls divided by the paced rate; Geocode work overlaps with it.

| Setting | States | Locations per call | Geo Tax calls per 1,000 addresses | Seconds per 100,000 addresses (one unit) |
|---|---|---|---|---|
| previous endpoint, unthrottled | 50 | 24.9 | 57 | 154 |
| batch endpoint, 36 calls/s total | NY, FL | 10.0 | 120 to 134 | 1,335 |
| batch endpoint, 48 calls/s total | TX, CA | 10.0 | 134 | 1,136 |

The new endpoint takes at most 10 locations per call instead of 25 and enforces 50 calls per second per key. Calls per address rose 2.3 times and the rate is capped, so units became about 7 to 9 times slower. The measured unit time matches the arithmetic to within 2%:

`seconds per 100k addresses = (Geo Tax calls per 100k addresses) / (calls per second per unit)`, for example `13,400 / 12 = 1,117` against 1,136 measured.

Throughput is therefore **units x `tax_rate_per_sec` x 10 / 1.34 addresses per second**. More units do not help while the API limit is the cap; a higher rate does, up to 50 calls per second in total.

## API limits (load tests)

Geo Tax only, 10 locations per call, real coordinates, no retries, open-loop request schedule, 2-minute steps. All tests ran when nothing else used the key.

| Calls per second | Result | 429 responses | p50 / p95 latency |
|---|---|---|---|
| 20 to 44 | clean | 0 | 57 to 59 / 68 to 75 ms (the first 90 s at 20/s had a slow tail: p99 1.7 s) |
| 48 (4 runs of 2 min, plus a 5 minute soak) | clean | 0 to 2 per 5,760 calls; 1 in the 14,400-call soak | 57 / 67 ms |
| 50 (3 runs of 2 min) | at the limit | 3 to 11 per 6,000 calls | 56 / 66 ms |
| 52 (stopped by the safety rule after 5 s, 261 calls) | over | 3.1% | 58 / 66 ms |
| 54 (3 runs of 2 min) | over | 7.35%, identical in all 3 | 55 / 64 ms |

- **The limit is 50 calls per second per key**, and sharp: at 54 calls per second the accepted rate was 50.0 per second in every run.
- **Latency does not rise** near the limit. The API rejects calls (HTTP 429), it does not slow down.
- **No autoscaling effect.** The limit did not move over about 20 minutes with 6 minutes above it.
- **Errors** other than 429: 25 in about 110,000 test calls (0.02%). 23 of them were in one 2-minute warm-up at 36 calls/s (the type was not recorded, and it did not repeat in later runs at higher rates), 1 was a malformed response (a 200 with the wrong number of results) and 1 was in the 48 calls/s soak.
- Geocode was never throttled and was not load tested separately; at 25 requests in flight per unit task it ran with 0 throttles over 15.5M requests.

The configured 4 units x 12 calls/s = 48 calls/s leaves 4% under the limit. In the TX and CA runs (48 calls/s) there were 368 throttled Geo Tax calls in about 4.6M (0.008%), each handled by the limiter.

## Incremental run (CDC)

First incremental run: release 2026.10.5 applied to the 52 states that were behind (the other 2 were already current). The change feed had 14,062 added or updated addresses for these states and 28 deletions of processed addresses.

| | Result |
|---|---|
| States processed | 52 (TX and CA skipped, already current) |
| Addresses sent to the APIs | 14,062 (0.007% of 194.5M) |
| API calls | 1,178 Geocode, 395 Geo Tax, 0 throttled |
| Deletions nulled | 28 of 28 |
| Time per state | 28 to 69 seconds (median 61); API time is negligible at 1,573 calls |
| Whole run | **98.7 minutes**: plan 0.5, 52 states 93.7, reports 4.6 |

**A run with nothing new** (all states at the newest release): `plan_states` plans no states, the loop is skipped, and only the reports refresh. Measured: **2.0 minutes** in total (plan 27 s, reports 92 s). That is the cost of a scheduled weekly run in a week without a release.

**Estimate for a typical release.** A release that touches N states costs about N minutes plus 2 to 5 minutes for the reports, plus the API time of the changed addresses (about 350 addresses per second, so 100,000 changed addresses take 5 minutes). The first two releases after the baseline (21,061 and 4,480 added, 12,480 and 979 updated, together 14,062 addresses for the states that needed them) were applied in 98.7 minutes.

## Tuning

| Goal | Change |
|---|---|
| Faster first run | nothing left under the 50 calls/s limit except `tax_rate_per_sec` 12 to 12.5 (50 per second in total; no margin) |
| Fewer throttles | `tax_rate_per_sec` 11 (44 per second, 12% margin) |
| Lighter load on the Geocode API | lower `geo_concurrency` (does not slow Geo Tax: Geocode work overlaps) |
| Shorter incremental runs | the per-state minute is job start-up; run states in parallel only if the API owners agree, since they share one key quota |

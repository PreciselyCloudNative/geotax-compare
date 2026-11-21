# APIs used

The pipeline calls two Precisely cloud APIs. Both use the same credentials and the same base URL. Numbers below were measured on this project (194.5M addresses, November 2026); see [performance.md](performance.md) for the full results and how they were measured.

| | Geocode | Geo Tax |
|---|---|---|
| Endpoint | `POST https://api.cloud.precisely.com/v1/geocode` | `POST https://api.cloud.precisely.com/v1/geo-tax/location/batch` |
| Input | up to **25 addresses** per request | up to **10 locations** (longitude, latitude) per request |
| Calls per address | 2 requests per 25 addresses (option 2 and option 3), so 0.08 | about 0.13 per address after de-duplication |
| Rate limit | none reached (0 throttles in 15.5M calls) | **50 calls per second per API key**, measured |
| Typical latency | about 0.8 s per 25-address request (derived) | median 57 ms, p95 66 ms |
| Concurrency in the pipeline | 25 in flight per unit task | paced at 12 calls/s per unit task, 4 units in parallel |

## Authentication

Every request carries `Authorization: Apikey <base64(key:secret)>`. The key and secret are read from a Databricks secret scope at run time (`dbutils.secrets`), never from code, parameters or tables. Defaults: scope `precisely`, keys `api_key` and `api_secret`; override with the job parameters `secret_scope`, `secret_key_access`, `secret_key_secret`. Error messages never include the header.

The key quota is **shared by everything that uses the key**. Do not run two pipelines on the same key at the same time.

## Geocode

One request carries up to 25 addresses and returns one response per address, in order. The pipeline sends every address twice, changing only the data option:

| Option | `customPreferences.DATA_OPTION` | Meaning |
|---|---|---|
| Option 2 | `enterprise` | enterprise address data |
| Option 3 | `premium` | premium address data |

All other preferences are identical so that the two results are comparable: `maxResults` 1, `returnAllInfo` true, `matchMode` STANDARD, fall back to street centroid, then postal, then geographic, `RETURN_H3HEX` true, address casing UPPER. The full preference object is `GEOCODE_PREFS_BASE` in `src/geotax_compare/job/unit_lib.py`.

Each address becomes one row with the status (`OK`, `NO_MATCH`, `API_ERROR`), score, longitude, latitude, location type, match code, precision level, matched address and H3 cell, for both options.

## Geo Tax

One request carries up to **10 locations** (the API rejects more with `Batch size must not exceed 10`) and returns one response per location. The response has **no record id**: `responses[i]` answers `locations[i]`, so the code maps the answers back by position and checks that the counts match (a mismatch is recorded as an error for the whole batch, never guessed).

Preferences used (`GEOTAX_PREFS`): special purpose district output `SPD`, `salesTaxRateType` GENERAL, `outputCasing` **MIXED**, `matchMode` CLOSE, Vertex cross-reference key, no auxiliary files. `outputCasing` must be set: when it is left out the API returns upper case, which would change every name column compared downstream.

**Coordinate de-duplication.** Geo Tax is called once per distinct coordinate, and the answer is used for both Geocode options. Where option 2 and option 3 return the same point, one lookup serves both. This cuts the lookups from 2.0 to about 1.34 per address.

Each address gets the tax result for option 2 and for option 3: confidence, match level, state / county / place codes and names, special districts, and the state, county, municipal and total sales tax rates.

## Constraints

| Constraint | Value | Where it comes from |
|---|---|---|
| Geocode batch size | 25 addresses | API maximum |
| Geo Tax batch size | 10 locations | API maximum |
| Geo Tax rate | 50 calls/s per key | measured: 54 calls/s gave 7.35% HTTP 429 in each of 3 runs (the accepted rate stayed at 50.0/s); 48 calls/s gave none |
| Geo Tax behaviour above the limit | HTTP 429, latency unchanged | measured: the API rejects, it does not slow down |
| Limit stays fixed over time | no autoscaling effect seen | measured: 3 repeats over about 20 minutes, 6 minutes of it above the limit |
| Settings in the jobs | 4 units x 12 calls/s = 48 calls/s | leaves 4% under the limit |

The 4% margin is thin on purpose (it is 33% faster than the earlier 36 calls/s). If throttling is frequent, lower `tax_rate_per_sec` to 11 (44 calls/s).

## How the pipeline protects the APIs

- **Rate limiter** (`AdaptiveLimiter`). Calls are spaced evenly at `tax_rate_per_sec` per unit task, so a fast API cannot be pushed over its limit by a burst. On an HTTP 429 the rate is halved; after 50 successful calls it recovers by 25% up to the configured rate. A separate cap on calls in flight halves and recovers the same way.
- **Retries.** HTTP 429, 500, 502, 503, 504 and connection errors are retried up to 3 times with exponential back-off (1 s, 2 s, 4 s, plus jitter, at most 30 s). Other 4xx answers are not retried: a batch is split into single records to find the one that failed, so one bad address does not fail 24 others.
- **Circuit breaker** (`CircuitBreaker`). After 5 consecutive outage failures (502, 503, 504, a connection error, or a 500 that says to try again later) every thread pauses: 30 s, 60 s, 120 s, 180 s, then 300 s, with jitter. One thread then probes the API; success closes the breaker. If a single outage lasts longer than 20 minutes the unit fails, and the job-level retry (up to 2 times, 5 minutes apart) or the next run resumes it. Nothing already written is repeated.
- **Billing.** About 4 API calls per address in the first run (2 Geocode requests per 25 addresses, plus the Geo Tax lookups). Retries add to this (about 0.05%).

## Not used

The pipeline does not call any other Precisely endpoint. The `address_book` tables are read through Unity Catalog, not an API.

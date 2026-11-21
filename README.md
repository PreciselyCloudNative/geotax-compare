# geotax_compare

Compares **Geocode option 2 (enterprise data)** with **option 3 (premium data)** and the **Geo Tax** result of each, for every US address in the `address_book` catalog, one state at a time. A Databricks Asset Bundle: two jobs, one Lakeflow pipeline, Delta tables in Unity Catalog.

**Incremental.** The first run of a state processes all of its addresses. Every later run processes only what changed in the source: added and updated addresses go through the APIs again, deleted ones are nulled. A state already at the newest release costs no API call.

```bash
export BUNDLE_VAR_catalog=<catalog>                      # always set: the processing state lives in this catalog
databricks bundle deploy -t dev --profile <profile>
databricks bundle run geotax_compare_job -t dev --profile <profile> --params states=AZ
```

## Documentation

| Doc | What is in it |
|---|---|
| [Architecture](docs/architecture.md) | data flow, jobs, tables, how incremental runs and deletes work, known limits |
| [APIs](docs/apis.md) | Geocode and Geo Tax endpoints, batch sizes, rate limit, protections (limiter, retries, circuit breaker) |
| [Performance and run times](docs/performance.md) | first run and incremental run times, API load tests, tuning |
| [Operations](docs/operations.md) | run, monitor, stop and resume, parameters, troubleshooting |
| [Deployment](docs/deployment.md) | dev and prod targets, secrets, run identity, cut-over from dev, repository rules |

## Layout

```
databricks.yml                 bundle: variables and the dev / prod targets
resources/                     the parent job, the per-state job, the reports pipeline
src/geotax_compare/job/        notebooks (plan, gate, unit, reconcile) and unit_lib.py (API client, parsing)
src/geotax_compare/transformations/   silver quarantine and gold reports (pipeline)
src/geotax_compare/tools/      test tooling, not deployed to prod
tests/                         unit tests (python tests/test_unit_lib.py)
```

## Status

The first full run (194.5M addresses, 54 states and territories, release 2026.10.5) is complete. Prod continues from it; see [Deployment](docs/deployment.md#cut-over-from-dev-prod-continues-from-release-2026105).

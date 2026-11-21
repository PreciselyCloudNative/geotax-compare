# Architecture

The pipeline compares **Geocode option 2 (enterprise data)** with **option 3 (premium data)** and the **Geo Tax** result of each, for every address in the `address_book` catalog. It works one US state at a time and is **incremental**: the first run of a state processes every address, later runs process only what changed in the source since.

## Source (`address_book`, read only)

| Object | Use |
|---|---|
| `silver.adf_usa_current` | one row per `PBKEY` (address id); read for the first run of a state (BASELINE) and to list the states |
| `silver.adf_usa_change_feed` | `ADDED`, `UPDATED` and `DELETED` rows with the `release_seq` that introduced them; a `DELETED` row is a tombstone with only the key and release fields (no `ADMIN1`) |
| `silver.adf_usa_releases` | the applied releases; the newest `release_seq` is the target of a run |

Each Precisely release is applied to one table, so only added, updated and deleted records carry the new release version. The source is never written to.

## What a run does per state

| Situation | Action | API calls |
|---|---|---|
| The state has never been processed | **BASELINE**: every current record. Refused unless `allow_baseline=true` (see below) | about 4 per address |
| The state is behind the newest release | **DELTA**: the feed rows with `watermark < release_seq <= newest`. `ADDED` and `UPDATED` go through the APIs again; `DELETED` records are nulled in `results` | only for the changed addresses |
| The state is at the newest release | **SKIP** | none |
| An earlier batch was loaded but not finished | **RESUME**: finish that batch first; newer releases are picked up by the next run | only for the missing addresses |

- **Watermark.** The `batch_seq` (equal to the source `release_seq`) of the state's last `DONE` row in `run_control`. It advances only after the verification described below passes.
- **Several releases at once.** A state that is two or more releases behind reads all of them together and keeps only the newest change per `PBKEY` (added then updated is updated; updated then deleted is deleted).
- **Deletes.** A tombstone has no state, so it is attributed to a state through its `PBKEY` in our own `results`. A delete for an address that was never processed is ignored. A deleted record keeps `PBKEY` and `ADMIN1`; every other column becomes NULL and `row_status = 'DELETED'`.
- **Baseline guard.** A first run bills every address (about 6 days of processing at the current API limit, see [performance.md](performance.md)). The state of the pipeline lives in the tables, not in the bundle, so deploying against the wrong catalog would look like "nothing was ever processed". Therefore a FULL baseline is refused unless the job parameter `allow_baseline` is `true`. `ALL` leaves never-processed states out of the plan and prints a warning.

## Jobs

```
geotax_compare_job   (parameter states = AZ | AZ,CA,TX | ALL)
  plan_states  ->  process_states (for each state, ONE at a time)  ->  publish_reports (pipeline)
                        |
                        +-- geotax_compare_state_job  (one run per state)
                              gate -> has_work -> has_pending_units -> process_units (4 units at a time) -> reconcile
```

- `plan_states` turns `states` into the list to loop over. For `ALL` it plans only the states behind the newest release, so a run with nothing new plans nothing and finishes in about a minute.
- A `for_each` task can wrap only one task, so each state is a run of a child job.
- States run one at a time because they share one API key quota and one results table.
- `publish_reports` runs even if a state failed (`ALL_DONE`); gold only shows states whose latest run is `DONE`.
- `gate` finds the watermark, decides the action, runs a preflight (secrets readable, one live call to each API) and loads `work_items`. The decision happens before the preflight, so a refused baseline makes no API call.
- `process_units` works on **units**: the addresses with the same `pmod(xxhash64(POSTALCODE), n_units)`, about 100,000 addresses each, 4 units at a time. A unit works in chunks of 5,000 addresses: Geocode option 2 and 3, then Geo Tax once per distinct coordinate, then append to `results_staging`. The unit writes its `unit_ledger` row last (row count and a `PBKEY` checksum).
- `reconcile` verifies the batch (every address has exactly one staged row, every unit has a matching ledger row), merges `results_staging` into `results`, nulls the deletes, logs to `reconcile_log` and marks `run_control` as `DONE`. If anything does not add up the state stays `LOADED` and the next run repeats only what is missing.

Every step is idempotent. A run can be cancelled at any time: finished units and chunks are kept, and at most the chunks in flight (about 20,000 addresses) are repeated.

## Tables (`<catalog>.geotax_compare_*`)

| Schema | Table | Content |
|---|---|---|
| bronze | `run_control` | one row per (state, run mode, batch): `LOADED` then `DONE`; the `DONE` `batch_seq` is the watermark |
| bronze | `work_items` | the addresses loaded for a batch (BASELINE, or ADDED / UPDATED / DELETED) with an `is_valid` flag |
| bronze | `unit_ledger` | one row per finished unit: counts, API calls, throttles, retries, seconds |
| silver | `results_staging` | append-only output of the units (keeps every batch) |
| silver | `results` | **current state**, one row per `PBKEY`: both Geocode results, both Geo Tax results, `row_status` |
| silver | `quarantine_records` | (pipeline) records that did not complete, with the reason |
| ops | `preflight_log`, `reconcile_log` | audit logs |
| gold | `geocode_tax_comparison`, `comparison_summary`, `distance_distribution`, `tax_differences`, `pipeline_funnel`, `quarantine_summary` | (pipeline) reports; `as_of_release` and `record_release` show the release |

`sample_size > 0` runs a deterministic sample and writes `*_sample` copies of work, staging and results; it never reaches gold.

Gold covers every state with a `DONE` full run, whichever states a given job run listed. The pipeline runs as the last task of a job run, or by hand.

## Known limits

- **Missing deletions.** A record that disappears from `adf_usa_current` without a `DELETED` row in the change feed stays in `results`. At release 2026.10.5 this affected 32 of 194.5M records (30 have no row in the change feed; 2, in TX, have a `DELETED` feed row at release 3 that the TX baseline did not apply). A reconcile sweep that marks results absent from the current table as deleted would close it; it is not built.
- A record whose `ADMIN1` changes between releases is processed in its new state; its old-state row is replaced when the new state's batch merges (same `PBKEY`).
- `results_staging` keeps every batch, so it grows with every delta and is about as large as `results` after the first run. Purging staging rows of finished batches is safe for correctness (reconcile only reads the current batch) but removes the audit trail.
- Gold `as_of_release` is per state; states can be at different releases until a run has brought them all to the newest one.
- The API key quota is shared by every pipeline that uses the key.

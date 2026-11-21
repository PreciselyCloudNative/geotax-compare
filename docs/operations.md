# Operations runbook

All commands use the Databricks CLI with a profile (`--profile <name>`) for the workspace you deployed to. Job ids come from `databricks jobs list`.

## Run the pipeline

| Goal | Command |
|---|---|
| Bring every state to the newest release (the normal run, also what the prod schedule does) | `databricks jobs run-now --profile <p> --json '{"job_id": <geotax_compare_job id>, "job_parameters": {"states": "ALL"}}'` |
| One or more states | same, with `"states": "AZ"` or `"states": "AZ,CA,TX"` (the states run one at a time, in the order given) |
| Process a state that has never been processed | add `"allow_baseline": "true"` (first run bills every address, see [performance.md](performance.md)) |
| Resume after a cancel or failure | run the same command again; the state resumes its unfinished batch |

`databricks bundle run` cannot carry a comma list in `--params` (it splits on commas), so use `jobs run-now --json` for lists.

Jobs queue: a run started while another is active waits its turn. Only one `geotax_compare_job` run is active at a time.

## Job parameters

| Parameter | Default | Meaning |
|---|---|---|
| `states` | `""` (dev), `ALL` (prod) | `AZ`, `AZ,CA,TX` or `ALL` |
| `allow_baseline` | `false` | allow a state with no finished run to be processed in full |
| `sample_size` | `0` | `>0` processes only that many records per state into `*_sample` tables; never reaches gold |
| `unit_size` | `100000` | addresses per unit |
| `geo_concurrency` | `25` | Geocode requests in flight per unit task |
| `tax_concurrency` | `6` | Geo Tax requests in flight per unit task |
| `tax_rate_per_sec` | `12` | Geo Tax calls per second per unit task; x 4 units = 48 of the 50 per second the API accepts |
| `source_current`, `source_feed`, `source_releases` | `address_book.silver.adf_usa_*` | source tables |
| `fail_after_chunks` | `0` | fault injection for tests; leave 0 |

Changing `tax_rate_per_sec` is the one setting that trades speed against throttling. Lower it to 11 if the throttle count in `unit_ledger` is not near zero.

## Check status

```sql
-- where every state stands (the DONE batch_seq is the watermark)
SELECT state, batch_seq, release_version, kind, status, rows_loaded, loaded_at, completed_at
FROM <catalog>.geotax_compare_bronze.run_control
WHERE run_mode = 'FULL' ORDER BY state, batch_seq;

-- progress of a running state
SELECT count(DISTINCT unit_id) AS units_done, max(n_units) AS n_units, sum(rows_written) AS rows,
       sum(tax_throttles) AS tax_429, sum(retries) AS retries
FROM <catalog>.geotax_compare_bronze.unit_ledger
WHERE state = 'TX' AND run_mode = 'FULL' AND status = 'COMPLETE' AND batch_seq = <batch_seq>;

-- did the last reconcile pass?
SELECT state, batch_seq, passed, rows_expected, rows_ok, rows_tax_failed, rows_geocode_failed, deletes_applied, detail
FROM <catalog>.geotax_compare_ops.reconcile_log ORDER BY checked_at DESC LIMIT 20;
```

`LOADED` in `run_control` means a batch is loaded but not finished: it is either running or was interrupted. It resumes on the next run.

## Alerts

`geotax_compare_job` mails `alert_email` on failure and on success. A failed state fails its loop iteration and therefore the job. Failures usually mean one of:

| Message | Cause | What to do |
|---|---|---|
| `baseline refused: no finished run exists for this state` | a never-processed state, or the **wrong catalog** | check `--var catalog` first; only if a first run is really intended, set `allow_baseline=true` |
| `Reconcile failed for XX: ... have no staged result row` | units did not all finish | run again; only the missing units are redone |
| `retries exhausted` / `API outage lasted longer than the ... budget` | an API outage longer than 20 minutes | wait for the API, run again (the unit resumes) |
| preflight failure (`secrets_available`, a live API call) | secret scope missing or unreadable, key rejected, API down | fix the scope or key; nothing billable ran |

## Stop and restart safely

Cancel the run with `databricks jobs cancel-run <run_id>` (cancel the parent; the child job and its tasks stop with it). Everything finished is kept in Delta; at most the chunks in flight (about 20,000 addresses per unit task) are repeated. Gold is built from `run_control`, so a cancelled run leaves gold consistent with the states that finished; refresh it by running the pipeline (`geotax_compare`) by hand.

To lower the load on the APIs for a while, cancel, then run again later with a lower `tax_rate_per_sec` and `geo_concurrency`.

## Reports

`publish_reports` refreshes `quarantine_records` and the gold tables at the end of a job run. To refresh them without processing anything, start the pipeline `geotax_compare` from the workspace or `databricks pipelines start-update <pipeline_id>`.

## Reprocess a state from scratch

Rarely needed (a delta already fixes changed records). It bills every address again.

```sql
DELETE FROM <catalog>.geotax_compare_bronze.run_control WHERE state = 'XX' AND run_mode = 'FULL';
```
then run the state with `allow_baseline=true`. The state's old rows in `results` are replaced by the merge. Do not do this for several large states at once: see the durations in [performance.md](performance.md).

## Housekeeping

- `results_staging` grows with every batch (see [architecture.md](architecture.md#known-limits)).
- `OPTIMIZE` the `results` table occasionally; it is liquid-clustered by (`ADMIN1`, `PBKEY`).
- Test tooling lives in `src/geotax_compare/tools/` (a synthetic next release for testing the delta path) and is not deployed to prod.

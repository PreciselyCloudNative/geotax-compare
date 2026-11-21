# Deployment

The project is a Databricks Asset Bundle (`databricks.yml`) with two targets.

| | `dev` | `prod` |
|---|---|---|
| Mode | `development` (resources prefixed with the user, schedules paused) | `production` |
| Bundle files | `/Workspace/Users/<you>/.bundle/geotax_compare/dev` | `/Workspace/Users/<deploying identity>/.bundle/geotax_compare/prod` |
| Schedule | none | weekly, Friday 22:30 Pacific time, **created paused**: unpause after the first prod run has been checked |
| `states` default | empty (you must say which) | `ALL` |
| Test tools (`src/geotax_compare/tools/`) | deployed | not deployed |

## What the bundle does not contain

- **No workspace URL.** Select the workspace with a Databricks CLI profile (`--profile`) or `DATABRICKS_HOST`.
- **No catalog.** The variable `catalog` defaults to `main`. Always pass the real one: `--var catalog=<catalog>` or `export BUNDLE_VAR_catalog=<catalog>`.
- **No credentials.** The API key and secret are read from a Databricks secret scope at run time.

The state of the pipeline lives in the tables of that catalog (`<catalog>.geotax_compare_*`), **not** in the bundle. Deploying a second copy against the same catalog continues from the same watermarks (that is how prod continues from dev); deploying against another catalog starts from nothing. The baseline guard (`allow_baseline`, see [architecture.md](architecture.md)) turns that mistake into a failed run instead of a 6 day bill.

## Prerequisites

1. The Databricks CLI (tested with v1.18), authenticated to the workspace (`databricks auth login --profile <name>`).
2. A Unity Catalog catalog where the deploying identity can create schemas (`geotax_compare_bronze`, `_silver`, `_ops`, `_gold`) and tables. The first run creates them.
3. `SELECT` on the source tables `address_book.silver.adf_usa_current`, `adf_usa_change_feed`, `adf_usa_releases`.
4. A secret scope with the Precisely API key and secret, readable by the identity that runs the jobs:

   ```bash
   databricks secrets create-scope precisely
   databricks secrets put-secret precisely api_key --string-value '<key>'
   databricks secrets put-secret precisely api_secret --string-value '<secret>'
   ```
   Different names are passed with the job parameters `secret_scope`, `secret_key_access`, `secret_key_secret`.
5. Serverless compute for jobs and pipelines in the workspace.

## Deploy

```bash
export BUNDLE_VAR_catalog=<catalog>
databricks bundle validate -t dev  --profile <profile> --strict
databricks bundle deploy   -t dev  --profile <profile>

databricks bundle validate -t prod --profile <profile> --strict
databricks bundle deploy   -t prod --profile <profile>
```

`alert_email` (success and failure mails) defaults to the deploying user; for prod pass a distribution list: `--var alert_email=<address>`.

## Run identity (prod)

Without `run_as` the jobs run as the identity that deployed them. For prod, deploy with a **service principal** so that runs do not depend on a person: authenticate the CLI as the service principal (OAuth machine-to-machine), give it the grants above and `READ` on the secret scope, and deploy. The bundle's `root_path` is under that identity's folder, so it is not writable by other users.

## Cut-over from dev: prod continues from release 2026.10.5

The first run processed all 194.5M addresses at a cost of about 66 hours of processing and the same API volume again if repeated. Prod must not redo it. Because the state is in the tables, prod uses the **same catalog** as dev:

1. Deploy prod with the catalog that holds the dev tables.
2. Check the watermarks before anything runs: every state `DONE` at the newest release.
   ```sql
   SELECT batch_seq, release_version, status, count(*) AS states
   FROM <catalog>.geotax_compare_bronze.run_control WHERE run_mode = 'FULL' GROUP BY ALL ORDER BY 1;
   ```
3. Run the prod job once by hand with `states=ALL`. With nothing new it plans no state, skips the loop and refreshes the reports in about 2 minutes. If a newer release has arrived it processes only the changes.
4. Unpause the schedule (`schedule.pause_status: UNPAUSED` in a pull request, or in the job settings).

### Move the report tables to the prod pipeline

A materialized view can belong to one pipeline only. If a dev pipeline already owns the silver `quarantine_records` and the six gold tables, the prod pipeline cannot write them until they are moved ([Databricks: move tables between pipelines](https://docs.databricks.com/aws/en/ldp/move-table)). Both pipelines must be idle and run as the same identity, in the same workspace. Once per table (7 tables), as that identity:

```sql
ALTER MATERIALIZED VIEW <catalog>.geotax_compare_silver.quarantine_records
  SET TBLPROPERTIES ('pipelines.pipelineId' = '<prod pipeline id>');
-- the same for geotax_compare_gold: comparison_summary, distance_distribution, geocode_tax_comparison,
-- pipeline_funnel, quarantine_summary, tax_differences
```

The data stays in place; the next update of the prod pipeline refreshes the views. After the move, do **not** run the dev pipeline or the dev job any more (they share the tables); remove the dev deployment with `databricks bundle destroy -t dev`. To undo a move, run the same statement with the other pipeline's id.

If the catalog is wrong, the plan finds no finished state, leaves every state out with a warning, and nothing is billed. Starting a state by name fails with `baseline refused`.

**Dev and prod share the tables.** After the cut-over, run `ALL` only from prod. Dev runs against the same catalog change the same watermarks and results; use `sample_size` (writes `*_sample` tables) or a separate catalog for experiments.

## Repository and review

`main` is protected by the `protect-main` ruleset: no deletion, no force push, and every change goes through a pull request with an approving review. CI (`.github/workflows/ci.yml`) runs the unit tests on every pull request. Bundle validation needs workspace access and is run by hand (`databricks bundle validate -t prod --strict`, which passes on a clean clone) before merging a change to `databricks.yml`, the jobs or the pipeline.

## Security

- No secret is stored in the repository, a job parameter, a table or a log. The key and secret are read with `dbutils.secrets` in the gate and unit notebooks and held in memory.
- The notebooks print the secret **scope name** only (`scope=<name>`), never a value.
- Error messages from the API client never include request headers.
- Anyone who can run the jobs can use the key through them: restrict `CAN_MANAGE_RUN` on the jobs and `READ` on the scope accordingly.

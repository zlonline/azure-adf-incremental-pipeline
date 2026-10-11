# Testing Guide — Azure Data Factory Incremental Pipeline

> Project: SQL Server → Azure Data Factory (ADF) → ADLS Gen2 → Parquet  
> Pipeline: `pl_customer_incremental`  
> Status: **Test plan / template**. Do not mark a test as passed until it has been executed and evidence has been captured.

## 1. Purpose and scope

Verify that the pipeline reads an incremental window from SQL Server, writes Parquet to ADLS Gen2, advances its SQL watermark only after successful copying, and exposes actionable monitoring information. This guide also checks scheduled execution, parameterization, security, and basic failure behavior.

**Important limitation:** A successful Copy Activity and a protected watermark do **not** establish exactly-once processing or complete data quality. Re-runs can create duplicate files, and timestamp-based extraction does not capture hard deletes.

## 2. Components under test

| Component | Name / setting |
|---|---|
| ADF pipeline | `pl_customer_incremental` |
| Full-load reference pipeline | `pl_customer_full_load` |
| Source | `dbo.customers` (SQL Server) |
| Control table | `dbo.pipeline_watermark` |
| Watermark key | `customer_pipeline` |
| Old watermark activity | `Get Old Watermark` |
| New watermark activity | `Get New Watermark` |
| Copy activity | `Copy Incremental Customers` |
| Success activity | `Update Watermark` |
| Failure activity | `Fail Customer Pipeline` |
| Stored procedure | `dbo.usp_update_pipeline_watermark` |
| SQL linked service | `ls_sqlserver_deproject` |
| SQL control dataset | `ds_sqlserver_control` |
| ADLS linked service | `ls_adls_deproject` |
| Parquet sink dataset | `ds_adls_customers_incremental` |
| Self-hosted IR | `ir_sqlserver_local` |
| Schedule trigger | `tr_customer_daily` |
| Optional Key Vault linked service | `ls_keyvault_deproject` (only if configured) |
| Destination | `raw/customers/year=YYYY/month=MM/day=DD/customers_<RunId>.parquet` |
| Copy retry policy | 3 retries; 30-second retry interval |

Pipeline parameters:

```text
schema_name      = dbo
table_name       = customers
watermark_column = updated_at
watermark_key    = customer_pipeline
```

## 3. Safety and preflight checklist

- [ ] Use a **development** SQL database, ADF, and storage account; never inject test faults into production.
- [ ] Pause `tr_customer_daily` during manual testing, and ensure no other run is using `customer_pipeline` concurrently.
- [ ] Confirm `ir_sqlserver_local` is online and SQL/ADLS linked-service connections work.
- [ ] Verify SQL source and watermark table exist, and record the original watermark.
- [ ] Use synthetic customer records only; do not expose personal information in screenshots or GitHub.
- [ ] Capture each ADF Run ID and its actual Old/New watermark values before evaluating results.
- [ ] Back up the working Copy query and linked-service/dataset settings before any negative test.
- [ ] Do not publish deliberately broken configuration. Restore and retest before enabling the trigger.

### Pre-test SQL checks

```sql
SELECT COUNT(*) AS total_rows,
       MIN(updated_at) AS min_updated_at,
       MAX(updated_at) AS max_updated_at
FROM dbo.customers;

SELECT pipeline_name, watermark_value
FROM dbo.pipeline_watermark
WHERE pipeline_name = 'customer_pipeline';

SELECT COUNT(*) AS null_updated_at_count
FROM dbo.customers
WHERE updated_at IS NULL;
```

### Expected incremental window

The implemented extraction rule is:

```sql
-- Illustrative SQL; the ADF pipeline supplies the actual values.
SELECT *
FROM dbo.customers
WHERE updated_at > @OldWM
  AND updated_at <= @NewWM;
```

For a **pre-run** estimate (before new source changes occur):

```sql
DECLARE @OldWM DATETIME2;
DECLARE @NewWM DATETIME2;

SELECT @OldWM = watermark_value
FROM dbo.pipeline_watermark
WHERE pipeline_name = 'customer_pipeline';

SELECT @NewWM = MAX(updated_at)
FROM dbo.customers;

SELECT @OldWM AS old_watermark, @NewWM AS new_watermark;

SELECT COUNT(*) AS expected_rows
FROM dbo.customers
WHERE updated_at > @OldWM AND updated_at <= @NewWM;
```

**Caution:** A post-run query using the *current* watermark will not reconstruct the completed run. Preserve the OldWM and NewWM from that run. Even with preserved bounds, live source-table changes can alter a later count.

## 4. Test case matrix

| ID | Scenario | Expected behavior | Evidence |
|---|---|---|---|
| TC-01 | Linked services / IR | SQL and ADLS connections pass; SHIR online | Connection tests, IR status |
| TC-02 | Old watermark lookup | Reads `customer_pipeline` watermark | Lookup output |
| TC-03 | New watermark lookup | Returns `MAX(updated_at)` | Lookup output |
| TC-04 | Incremental copy with new records | Reads only `OldWM < updated_at <= NewWM` | Copy input/output, SQL validation |
| TC-05 | Parquet destination | New run-specific Parquet output in date folder | ADLS browser, Run ID |
| TC-06 | Success dependency | Watermark updated to this run's NewWM | Stored procedure activity, SQL query |
| TC-07 | No new records | Zero eligible rows; no incorrect advancement | Lookup/copy output, SQL query |
| TC-08 | Copy-specific SQL error | Copy fails; Fail Activity runs; watermark unchanged | Failed activity, SQL watermark |
| TC-09 | Invalid table parameter | New lookup fails; pipeline fails; watermark unchanged | Lookup error, SQL watermark |
| TC-10 | Invalid ADLS file system / access | Copy fails; watermark unchanged | Copy error, SQL watermark |
| TC-11 | SQL connection failure | First SQL-dependent activity fails | Lookup/connection error |
| TC-12 | Retry configuration | Copy policy has Retry=3 and interval=30s | Copy policy screenshot |
| TC-13 | Schedule trigger | Correct pipeline/parameters/time zone and published state | Trigger settings, triggered run |
| TC-14 | Security | Identity/storage access and optional Key Vault connection verified | Sanitized settings / test result |
| TC-15 | Monitoring | Run ID, status, duration, metrics and errors accessible | ADF Monitor screenshots |

## 5. Positive end-to-end test (TC-01 through TC-06)

1. Pause the schedule trigger and record the current SQL watermark.
2. Ensure the source contains synthetic records with `updated_at` greater than OldWM. If necessary, create them **only in the development database**, respecting the existing `dbo.customers` schema.
3. Before starting the run, record the expected count using the SQL in section 3.
4. In ADF Studio, open `pl_customer_incremental` and choose **Debug** with the four parameters shown above.
5. In **Monitor → Pipeline runs → Activity runs**, verify that `Get Old Watermark`, `Get New Watermark`, `Copy Incremental Customers`, and `Update Watermark` succeed.
6. Inspect Lookup outputs to capture the run's actual OldWM and NewWM. Inspect Copy **Input**, **Output**, `rowsRead`, `rowsCopied`, and `copyDuration`.
7. In ADLS Gen2, check the `raw` file system and `customers/year=YYYY/month=MM/day=DD/` directory. Confirm the expected `customers_<RunId>.parquet` object exists. The partition date is based on `utcNow()` in the current design, not necessarily Toronto local date or customer event date.
8. Query the SQL control table again. Confirm `watermark_value` equals the run's NewWM **if the stored procedure succeeded**.
9. Save sanitized screenshots and the Run ID.

**Pass criteria:** All required activities succeed, eligible records are reported copied, the intended Parquet object exists, and the watermark matches the NewWM for that run. `rowsRead = rowsCopied` is useful but is **not** a complete data-quality assertion.

## 6. No-new-data test (TC-07)

1. After a successful run, do not modify `dbo.customers`.
2. Record the current watermark and run the pipeline again.
3. Confirm OldWM equals NewWM (assuming source `MAX(updated_at)` is non-null and stable).
4. Confirm zero eligible rows and no unexpected watermark change. Depending on sink behavior, an empty Parquet file may or may not be produced.

**Pass criteria:** No new business records are ingested and the control watermark is not incorrectly advanced.

**Edge case:** If `dbo.customers` is empty, `MAX(updated_at)` is `NULL`. The existing training pipeline may require an additional guard before it can safely handle this case; record it as a known limitation, not a passed test.

## 7. Negative tests (TC-08 through TC-11)

### TC-08 — Force a Copy-only SQL failure

1. Record OldWM. Keep both Lookup activities correctly configured.
2. Temporarily change **only the Copy Activity source query** to reference `nonexistent_column` from `dbo.customers`.
3. Run Debug and inspect the execution graph.
4. Expected: both Lookups succeed; Copy fails; `Fail Customer Pipeline` executes; `Update Watermark` is skipped; the SQL watermark remains unchanged.
5. Restore the original dynamic Copy query and run a successful Debug before publishing anything.

> Changing the shared `watermark_column` pipeline parameter may fail `Get New Watermark` **before** Copy; that does not test the Copy failure dependency.

### TC-09 — Invalid source table parameter

Debug with `table_name = customers_wrong` (keep `watermark_key = customer_pipeline`). Expected: `Get New Watermark` fails with an invalid-object-type SQL error; Copy does not execute; watermark is unchanged. Restore `customers`.

### TC-10 — Invalid ADLS destination

In a **development-only** dataset/test copy, temporarily set the sink file system to an invalid/nonexistent container (for example `raw_missing_test`), or test an intentionally denied identity in an isolated environment. Expected: Copy fails and watermark does not advance. Restore the original `raw` setting.

> Changing only a folder name often **does not** cause failure: ADLS may create a new folder. Do not remove production RBAC assignments to simulate this test.

### TC-11 — SQL connection failure

Use an isolated test linked service with an intentionally invalid server address or temporarily disable the test connection. Verify the linked-service Test Connection or the first SQL-dependent Lookup fails. Restore the valid `ls_sqlserver_deproject` connection. Do not alter a shared production linked service.

## 8. Trigger, retry, and monitoring checks (TC-12 through TC-15)

- **Retry:** Inspect **Copy Activity → General → Policy**: Retry `3` means up to **three additional attempts** after the initial attempt; interval `30` seconds. Permanent errors will generally continue to fail despite retries.
- **Trigger:** Confirm `tr_customer_daily` is published, targets `pl_customer_incremental`, supplies all four parameters, and uses the intended **Eastern Time** zone. A daily 2:00 AM schedule needs an explicit daylight-saving-time review because local 2:00 AM can be skipped or repeated around DST transitions.
- **Monitoring:** Record Pipeline Run ID, Activity Run status, duration, source and sink information, `rowsRead`, `rowsCopied`, and Error details if applicable.
- **Security:** Verify ADF Managed Identity can write to ADLS using appropriate RBAC and (where applicable) ADLS ACL. If configured, test `ls_keyvault_deproject` and confirm the SQL linked service references the Key Vault secret without exposing its value.
- **After testing:** Restore valid settings, publish approved changes, and only then restart the schedule trigger.

## 9. Test execution log (fill in with real results)

| Test ID | Date (UTC) | ADF Run ID | OldWM | NewWM | Rows read/copied | Result (Pass/Fail/Not Run) | Evidence |
|---|---|---|---|---|---|---|---|
| TC-01 | TBD | N/A | N/A | N/A | N/A | Not Run | TBD |
| TC-04 | TBD | TBD | TBD | TBD | TBD | Not Run | TBD |
| TC-06 | TBD | TBD | TBD | TBD | TBD | Not Run | TBD |
| TC-08 | TBD | TBD | TBD | TBD | N/A | Not Run | TBD |
| TC-13 | TBD | TBD | N/A | N/A | N/A | Not Run | TBD |

## 10. Acceptance checklist

- [ ] All critical positive test cases have actual evidence.
- [ ] Watermark update occurs only after successful Copy Activity completion.
- [ ] Failure test demonstrates no watermark advancement when Copy fails.
- [ ] ADLS output path and run-specific Parquet file are verified.
- [ ] SQL and ADLS connectivity, SHIR, and required permissions are verified.
- [ ] Trigger configuration, retry policy, and ADF Monitor are reviewed.
- [ ] Fault injection is removed; all development settings are restored.
- [ ] No secrets or personal data are present in repository artifacts.
- [ ] Known limitations are documented rather than represented as resolved.

## 11. Suggested evidence files

```text
screenshots/
  01_pipeline_overview.png
  02_successful_run.png
  03_failure_handling.png
  04_monitoring.png
  05_adls_parquet.png
```

See [`troubleshooting.md`](troubleshooting.md) for incident triage and recovery procedures.

# Troubleshooting Runbook — Azure Data Factory Incremental Pipeline

> Pipeline: `pl_customer_incremental`  
> Source: SQL Server `dbo.customers`  
> Sink: ADLS Gen2 Parquet  
> Control table: `dbo.pipeline_watermark`, key `customer_pipeline`

## 1. Objective

Diagnose and safely recover failed or suspicious ADF runs. The operational question is: **“Why did last night's 2:00 AM incremental pipeline fail, and is it safe to rerun?”**

This is a practical runbook, not evidence that any particular incident occurred. Use development environments for deliberate fault-injection exercises.

## 2. Architecture and failure boundaries

```text
tr_customer_daily (2:00 AM Eastern)
           |
           v
pl_customer_incremental
           |
           +--> Get Old Watermark --> dbo.pipeline_watermark
           |
           +--> Get New Watermark --> MAX(dbo.customers.updated_at)
           |
           v
Copy Incremental Customers (OldWM < updated_at <= NewWM)
           |
           +--> SQL Server via ir_sqlserver_local
           +--> ADLS Gen2 raw/customers/year=YYYY/month=MM/day=DD/
           |
           +-- Success --> Update Watermark (stored procedure)
           |
           +-- Failure --> Fail Customer Pipeline
```

Security: ADF Managed Identity + ADLS RBAC/ACL; optionally ADF Managed Identity → Key Vault (`ls_keyvault_deproject`) → SQL password used by the SQL linked service.

**Boundary to remember:** The explicit `Fail Customer Pipeline` branch is attached to **Copy Activity failure**. A failure in an upstream Lookup may fail the pipeline without executing that Fail Activity. A failure in `Update Watermark` may occur **after** Parquet has been written.

## 3. First-response checklist

1. **Identify the run:** ADF Studio → **Monitor → Pipeline runs**; filter by `pl_customer_incremental`, date/time, and status.
2. **Record evidence:** Pipeline Run ID, trigger type, parameters, start/end times, duration, status. Check whether 2:00 AM is displayed in local or UTC time.
3. **Open Activity runs:** Find the **first failed activity**, not just the final red node.
4. **Inspect Input, Output, Error:** Capture error code/message, SQL query or sink path (redact credentials), row counts, retry details, and integration runtime information.
5. **Classify the failure:** Source SQL, watermark lookup, SHIR/network, Key Vault/authentication, ADLS access/path, Copy runtime, stored procedure, or scheduling.
6. **Check side effects before rerunning:** Did a Parquet file already appear? Did `dbo.pipeline_watermark` advance? Is another run still active?
7. **Fix the root cause** in a controlled development/test configuration; validate the fix.
8. **Rerun only after assessing duplicate risk** and preserving evidence. Record the resolution and final state.

## 4. SQL diagnostics

### Current watermark and source maximum

```sql
SELECT pipeline_name, watermark_value
FROM dbo.pipeline_watermark
WHERE pipeline_name = 'customer_pipeline';

SELECT MAX(updated_at) AS current_source_max
FROM dbo.customers;
```

### Estimate eligible rows for the current window

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
WHERE updated_at > @OldWM
  AND updated_at <= @NewWM;
```

**Important:** This is a **current-state** check, not a reliable reconstruction of a previous run. For an earlier run, use its captured Lookup outputs (`OldWM` and `NewWM`) and account for any source changes since then.

### Source sanity checks

```sql
SELECT COUNT(*) AS null_updated_at_count
FROM dbo.customers
WHERE updated_at IS NULL;

SELECT TOP (20) *
FROM dbo.customers
ORDER BY updated_at DESC;
```

## 5. Symptom-to-cause reference

| Symptom / error | Likely cause | Investigation | Safe action |
|---|---|---|---|
| `Invalid object name 'dbo.customers_wrong'` | Bad `table_name` parameter | Inspect `Get New Watermark` Input and parameters | Restore `table_name=customers`; Debug |
| `Invalid column name` | Wrong column or deliberately broken Copy SQL | Identify which activity failed; inspect SQL | Correct only the affected SQL/parameter |
| SQL login failed | Incorrect credentials or expired password | SQL linked service Test Connection; Key Vault secret version | Verify SQL login and secret without exposing values |
| SQL connection timeout | SHIR offline, DNS, firewall, network, SQL down | SHIR status; host reachability; SQL service | Restore connectivity; retest |
| Key Vault 403 / Forbidden | ADF identity lacks secret read permission | Key Vault permission model, RBAC/access policy | Grant least-privilege read to the correct identity |
| Secret not found | Wrong secret name/version or disabled secret | Key Vault Secrets and linked service reference | Fix name/version; confirm secret enabled |
| ADLS 403 / authorization failure | Missing Storage RBAC and/or ACL | Managed Identity, IAM, ADLS ACL | Correct scoped access in dev/test |
| File system/container not found | Incorrect sink file system | Copy Input and dataset Connection | Restore `raw` file system |
| Unexpected new folder but no failure | Folder path typo; auto-creation | Inspect ADLS paths and dataset params | Correct folder path; review misplaced files |
| `rowsRead = 0` | No new records, wrong watermark, bad filter | Compare actual OldWM/NewWM and SQL count | Treat as normal if no eligible rows; otherwise investigate |
| `rowsRead > rowsCopied` | Row handling, sink failure, connector behavior | Copy Output, logs, errors | Investigate before treating run as successful |
| Copy succeeded; SP failed | Watermark update error | `Update Watermark` Error; SQL permissions | Assess duplicate-file risk before rerun |
| Trigger did not run | Stopped/unpublished trigger, timezone/DST | Manage → Triggers; Monitor | Fix schedule; publish/start after validation |
| Unexpected duplicate Parquet files | Rerun after partial/successful copy | Compare Run IDs, file paths, watermark | Reconcile data; do not assume exactly-once |

## 6. Failure scenario A — Wrong table name

**Reproduction in development:** Debug with `table_name=customers_wrong` and `watermark_key=customer_pipeline`.

Expected flow:

```text
Get Old Watermark     Succeeded
Get New Watermark     Failed (invalid table)
Copy Incremental      Not executed
Update Watermark      Not executed
Watermark             Unchanged
```

**Root cause:** The dynamic `MAX(updated_at)` lookup queries a nonexistent table.

**Recovery:** Correct `table_name`, validate `dbo.customers` exists, rerun Debug, and check the watermark. Do not mislabel this as a Copy failure test.

## 7. Failure scenario B — ADLS path / permissions

**Reproduction in development:** Temporarily point the sink dataset to a nonexistent file system such as `raw_missing_test`, or use a separately scoped test identity without access.

Expected flow: Lookups succeed; Copy fails; failure branch executes; watermark is unchanged.

**Investigation:** Check Copy Input for the file system and path, Copy Error for authorization/not-found details, Storage Account IAM, and ADLS Gen2 ACLs where applicable.

**Recovery:** Restore `raw` and the correct dataset parameters; retest. A wrong **folder** alone may be created automatically and therefore may not fail. Do not remove production permissions as a test.

## 8. Failure scenario C — SQL connectivity

**Reproduction in development:** Test a separate SQL linked service with an invalid hostname or unavailable test server.

Expected flow: Connection Test or first SQL-dependent Lookup fails before Copy starts.

**Investigation order:**

1. Confirm `ir_sqlserver_local` is **Online**.
2. Check SQL Server hostname, instance, port, DNS, firewall, and SQL service status.
3. Verify authentication mode, username, and password source.
4. If password comes from Key Vault, verify ADF Managed Identity and secret read permissions.
5. Use **Test Connection** before retrying the pipeline.

**Recovery:** Restore the correct linked service and connectivity. Avoid modifying a shared production linked service for a failure demonstration.

## 9. Failure scenario D — Copy-only SQL error

**Purpose:** Prove that the Copy failure dependency runs and the watermark does not advance.

In a development copy of the pipeline, leave both Lookup queries valid and temporarily use an invalid column **only in the Copy source SQL**, e.g.:

```sql
SELECT nonexistent_column FROM dbo.customers;
```

Expected:

```text
Get Old Watermark        Succeeded
Get New Watermark        Succeeded
Copy Incremental         Failed
Fail Customer Pipeline   Executed / Failed
Update Watermark         Skipped
SQL watermark            Unchanged
```

Restore the original parameterized Copy query, run Debug successfully, then publish only the corrected version.

## 10. Failure scenario E — Copy succeeds, watermark update fails

This is an especially important recovery case:

```text
Copy Incremental Customers  Succeeded
        |
        v
Parquet file may exist in ADLS
        |
        v
Update Watermark            Failed
```

The SQL watermark may still be at OldWM even though the data was written. Blindly rerunning can produce another Parquet file with the same records because the filename uses a different `pipeline().RunId`.

**Before rerun:**

1. Confirm whether the output file exists and whether its contents were committed.
2. Check the stored procedure error and SQL permissions.
3. Compare the failed run's OldWM/NewWM and the files already present.
4. Choose an explicit reconciliation approach (e.g., isolate/delete only verified duplicate development files, or deduplicate downstream) before reprocessing.
5. Never manually advance a production watermark solely to make a pipeline appear successful; reconcile the actual loaded data first.

**Design limitation:** Copy and SQL watermark update are not one atomic transaction. The current design provides at-least-once-like recovery behavior, not guaranteed exactly-once ingestion.

## 11. Rows read vs rows copied

| Observation | Interpretation | Next step |
|---|---|---|
| 100 / 100 | Copy reports matching source and sink row counts | Verify path, watermark, and quality separately |
| 0 / 0 | Often a valid no-change window | Confirm OldWM = NewWM and source maximum |
| 100 / 95 | Possible partial processing or row-handling issue | Inspect Copy Output, errors, and sink |
| 10,000 / 10,000 when normally 100 | Unexpected source volume | Check source changes, watermark, and parameters |

These are examples, **not actual project measurements**.

## 12. Retry policy and failure categories

Copy policy:

```text
Retry          = 3  (up to three additional attempts)
Retry interval = 30 seconds
```

| Potentially transient | Usually permanent until corrected |
|---|---|
| Brief network interruption | Invalid table / column |
| Temporary service throttling | Wrong secret name |
| Intermittent service availability | Missing RBAC / ACL |
| Short-lived connectivity issue | Incorrect SQL query |

Retries do not guarantee rollback of partially written files. Inspect side effects before a manual rerun.

## 13. Security troubleshooting

### Managed Identity and ADLS

- Identify the ADF system-assigned Managed Identity in the Data Factory **Identity** page.
- Confirm appropriate Storage Blob Data Contributor (or a more restrictive suitable role) at the correct scope.
- For hierarchical-namespace access, check ADLS Gen2 ACLs when applicable.

### Key Vault

- Verify `ls_keyvault_deproject` exists and points to the correct vault.
- Check whether the vault uses **Azure RBAC** or **Vault access policy** permissions.
- For RBAC, the ADF identity generally needs `Key Vault Secrets User` to read secrets; for access policies, it needs appropriate Secret Get permission.
- Verify `sql-server-password` exists, is enabled, and is referenced by `ls_sqlserver_deproject` if Key Vault integration was completed.
- Do not copy secret values into logs, tickets, screenshots, or GitHub.

### SHIR

- Verify `ir_sqlserver_local` is online and has network access to SQL Server.
- Distinguish SHIR/network failures from SQL authentication failures by checking connection-test diagnostics and the first failing SQL-dependent activity.

## 14. Trigger and timezone troubleshooting

- Check `tr_customer_daily` status, publication, target pipeline, and parameter mapping.
- Confirm schedule timezone is **Eastern Time** if the requirement is Toronto local time.
- A 2:00 AM local schedule can be affected by daylight-saving transitions. Inspect actual UTC run timestamps and the intended DST behavior rather than assuming every local date has an unambiguous 2:00 AM.
- Verify no overlapping run is advancing the same `customer_pipeline` watermark. The training pipeline does not implement a full concurrency-control strategy.

## 15. Incident report template

```markdown
### Incident: [short descriptive title]
- Incident date/time (with timezone):
- Environment: Development / Test / Production
- ADF pipeline:
- Pipeline Run ID:
- Trigger / parameters:
- First failed activity:
- Error code and sanitized message:
- OldWM / NewWM for the run:
- Rows read / copied:
- ADLS files created (if any):
- SQL watermark before / after:
- Root cause:
- Impact / duplicate risk:
- Corrective action:
- Rerun decision and validation:
- Preventive improvement:
- Evidence links:
```

### Example incident (fictional)

- **Title:** Invalid customer table parameter
- **First failure:** `Get New Watermark`
- **Error:** Invalid SQL object name
- **Cause:** `table_name=customers_wrong`
- **Impact:** No Copy Activity executed; watermark unchanged
- **Resolution:** Restored `table_name=customers`; validated and reran in development
- **Prevention:** Validate allowed source table names before execution

## 16. Operational recovery checklist

- [ ] Failed Run ID and first failing activity identified.
- [ ] Error and relevant Input/Output captured without secrets.
- [ ] OldWM/NewWM for the affected run preserved where available.
- [ ] Existing Parquet output and watermark side effects assessed.
- [ ] Root cause corrected and validated in a safe environment.
- [ ] Duplicate or partial-load risk assessed before rerun.
- [ ] Rerun monitored and final watermark/data state verified.
- [ ] Trigger re-enabled only after restoring valid published configuration.
- [ ] Incident report and preventive action documented.

## 17. Production hardening backlog

- Add explicit handling for empty source / NULL NewWM.
- Add idempotent ingestion or downstream MERGE/deduplication.
- Evaluate SQL Server CDC/Change Tracking for deletes and late commits.
- Add concurrency controls around the watermark key.
- Use deterministic run timestamps and clear timezone conventions for partitions.
- Add row-level data-quality checks and reconciliation.
- Configure alerting, centralized logging, and deployment automation.
- Restrict dynamic schema/table/column parameters to an approved allowlist.

See [`testing.md`](testing.md) for reproducible tests and acceptance criteria.

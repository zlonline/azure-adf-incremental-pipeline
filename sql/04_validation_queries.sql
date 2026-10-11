/*
Project: Azure Data Factory Incremental Data Pipeline
File: sql/04_validation_queries.sql
Purpose: Read-only SQL Server validation queries for Week 7 / Day 7.

Pipeline: pl_customer_incremental
Source: dbo.customers
Control: dbo.pipeline_watermark
Watermark key: customer_pipeline
Window: OldWM < updated_at <= NewWM

HOW TO USE
1. Run sections 1-5 BEFORE an ADF Debug/Trigger run. Record the old/new
   watermark values and expected row count for that run.
2. Run sections 6-8 AFTER the run. Compare the actual control watermark
   with the NewWM captured BEFORE the run and the ADF Activity output.
3. Use section 9 to validate a FAILED COPY: watermark must not advance.
4. Section 10 is a template for checking a HISTORICAL run. Fill in the
   actual OldWM and NewWM from THAT run, not the current control table.

IMPORTANT
- This script does not change data or update the watermark.
- Run against the same SQL Server database used by the ADF linked service.
- Counts can change during concurrent writes; the pipeline's source data
  is not guaranteed to be a transactionally consistent snapshot.
- ADF rowsRead/rowsCopied equality does NOT establish business data quality.
- A timestamp watermark does not capture physical DELETE operations.
- Never publish passwords, customer data, or sensitive server names to GitHub.
*/

SET NOCOUNT ON;

/* 1. Environment / source table checks */
SELECT DB_NAME() AS database_name, @@SERVERNAME AS server_name, SYSDATETIME() AS sql_server_local_time, SYSUTCDATETIME() AS sql_server_utc_time;

SELECT
    OBJECT_ID(N'dbo.customers', N'U') AS customers_object_id,
    OBJECT_ID(N'dbo.pipeline_watermark', N'U') AS watermark_object_id,
    OBJECT_ID(N'dbo.usp_update_pipeline_watermark', N'P') AS watermark_procedure_object_id;

SELECT
    c.name AS column_name,
    TYPE_NAME(c.user_type_id) AS data_type,
    c.is_nullable
FROM sys.columns AS c
WHERE c.object_id = OBJECT_ID(N'dbo.customers', N'U')
ORDER BY c.column_id;

/* 2. Source table overview */
SELECT
    COUNT_BIG(*) AS total_source_rows,
    MIN(updated_at) AS min_updated_at,
    MAX(updated_at) AS max_updated_at,
    SUM(CASE WHEN updated_at IS NULL THEN CONVERT(bigint, 1) ELSE CONVERT(bigint, 0) END) AS null_updated_at_rows
FROM dbo.customers;

/* 3. Current watermark state (record BEFORE a run) */
SELECT pipeline_name, watermark_value
FROM dbo.pipeline_watermark
WHERE pipeline_name = 'customer_pipeline';

/* 4. Calculate expected incremental window BEFORE a run */
DECLARE @OldWM datetime2;
DECLARE @NewWM datetime2;

SELECT @OldWM = watermark_value
FROM dbo.pipeline_watermark
WHERE pipeline_name = 'customer_pipeline';

SELECT @NewWM = MAX(updated_at)
FROM dbo.customers;

SELECT
    @OldWM AS old_watermark_before_run,
    @NewWM AS new_watermark_before_run,
    CASE
        WHEN @OldWM IS NULL THEN 'ERROR: missing control row or NULL old watermark'
        WHEN @NewWM IS NULL THEN 'WARNING: source MAX(updated_at) is NULL'
        WHEN @NewWM < @OldWM THEN 'WARNING: source maximum is older than watermark'
        WHEN @NewWM = @OldWM THEN 'NO NEW TIMESTAMP WINDOW'
        ELSE 'READY'
    END AS window_status;

SELECT COUNT_BIG(*) AS expected_incremental_rows
FROM dbo.customers
WHERE updated_at > @OldWM
  AND updated_at <= @NewWM;

/* 5. Inspect sample rows in the calculated window */
SELECT TOP (20) *
FROM dbo.customers
WHERE updated_at > @OldWM
  AND updated_at <= @NewWM
ORDER BY updated_at ASC;

/* 6. AFTER the ADF run: current watermark and source maximum */
SELECT
    w.pipeline_name,
    w.watermark_value AS watermark_after_run,
    (SELECT MAX(updated_at) FROM dbo.customers) AS current_source_max_updated_at
FROM dbo.pipeline_watermark AS w
WHERE w.pipeline_name = 'customer_pipeline';

/* 7. AFTER the ADF run: compare against PRE-RUN captured NewWM.
   Replace the example timestamp with the actual NewWM from section 4.
   Do NOT assume the current MAX(updated_at) is the same as the run's NewWM.
*/
DECLARE @ExpectedNewWM datetime2 = '2026-10-10T09:30:00'; -- REPLACE

SELECT
    w.pipeline_name,
    w.watermark_value AS actual_watermark,
    @ExpectedNewWM AS expected_watermark,
    CASE
        WHEN w.watermark_value = @ExpectedNewWM THEN 'PASS: watermark matches captured NewWM'
        ELSE 'REVIEW: watermark differs from captured NewWM'
    END AS validation_result
FROM dbo.pipeline_watermark AS w
WHERE w.pipeline_name = 'customer_pipeline';

/* 8. Basic source data-quality checks (review business requirements).
   Only use the customer_id uniqueness query if customer_id is expected
   to be unique in your source schema.
*/
SELECT COUNT_BIG(*) AS null_updated_at_rows
FROM dbo.customers
WHERE updated_at IS NULL;

SELECT customer_id, COUNT_BIG(*) AS duplicate_count
FROM dbo.customers
GROUP BY customer_id
HAVING COUNT_BIG(*) > 1;

/* 9. FAILED COPY validation.
   Record @WatermarkBeforeFailure BEFORE triggering the intentional
   Copy failure; replace the example below with that exact value.
   The watermark should be unchanged when Copy fails and Update Watermark
   is correctly connected to the Copy Success dependency.
*/
DECLARE @WatermarkBeforeFailure datetime2 = '2026-10-09T10:00:00'; -- REPLACE

SELECT
    w.watermark_value AS watermark_after_failed_copy,
    @WatermarkBeforeFailure AS watermark_before_failed_copy,
    CASE
        WHEN w.watermark_value = @WatermarkBeforeFailure
            THEN 'PASS: failed Copy did not advance watermark'
        ELSE 'REVIEW: watermark changed; check overlapping runs and dependencies'
    END AS failure_test_result
FROM dbo.pipeline_watermark AS w
WHERE w.pipeline_name = 'customer_pipeline';

/* 10. HISTORICAL run validation.
   Replace both example values with the ACTUAL OldWM and NewWM recorded
   in the ADF Lookup outputs for the SAME pipeline Run ID.
   Comparing with today's watermark is incorrect for historical runs.
*/
DECLARE @RunOldWM datetime2 = '2026-10-09T10:00:00'; -- REPLACE
DECLARE @RunNewWM datetime2 = '2026-10-10T09:30:00'; -- REPLACE

SELECT
    @RunOldWM AS run_old_watermark,
    @RunNewWM AS run_new_watermark,
    COUNT_BIG(*) AS current_source_rows_matching_historical_window
FROM dbo.customers
WHERE updated_at > @RunOldWM
  AND updated_at <= @RunNewWM;

/* NOTE: This is a CURRENT query of a HISTORICAL timestamp range, not a
   snapshot of what ADF read. Changes/deletes since that run can make the
   count differ from ADF's historical rowsRead/rowsCopied metrics.

   ADLS Parquet file existence, exact destination paths, copy metrics,
   retries, and error details must be checked in Azure Storage and ADF
   Monitor; SQL Server alone cannot validate those facts.
*/

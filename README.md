# azure-adf-incremental-pipeline

Azure Data Factory Incremental Data Pipeline

1. Project Overview

This project demonstrates an incremental data ingestion pipeline built with Azure Data Factory (ADF), SQL Server, and Azure Data Lake Storage Gen2.

The pipeline extracts newly inserted or updated records from SQL Server using a watermark-based loading strategy and stores them in ADLS Gen2 as Parquet files.

2. Architecture

SQL Server → ADF Watermark Lookups → Incremental Copy → ADLS Gen2 → Parquet

The pipeline uses a SQL Server control table to track the last successfully processed watermark.

3. Technologies

SQL Server

Azure Data Factory

Azure Data Lake Storage Gen2

Parquet

Self-hosted Integration Runtime

Azure Managed Identity

Azure RBAC

Azure Key Vault (if configured)

4. Key Features

Full and incremental data ingestion

SQL-based watermark tracking

Parameterized ADF pipelines

Dynamic SQL queries

Date-partitioned Parquet output

Daily scheduled execution

Retry and failure handling

Watermark updates after successful copying

Pipeline monitoring and troubleshooting

5. Incremental Loading Logic

The pipeline retrieves the previous watermark from a SQL Server control table and calculates the current maximum updated_at value.

Records are extracted using:

SELECT *
FROM dbo.customers
WHERE updated_at > @OldWatermark
  AND updated_at <= @NewWatermark;

After a successful Copy Activity, the watermark control table is updated.

If the Copy Activity fails, the watermark is not advanced.

6. ADLS Output Structure

raw/
└── customers/
    └── year=YYYY/
        └── month=MM/
            └── day=DD/
                └── customers_<RunId>.parquet

7. Monitoring and Troubleshooting

Azure Data Factory Monitor is used to review pipeline execution status, activity runs, duration, row counts, inputs, outputs, and error details.

The project includes troubleshooting scenarios for invalid SQL objects, incorrect storage paths, and SQL Server connection failures.

8. Testing

The pipeline is tested for:

Successful incremental loading

Correct Parquet output

Watermark advancement after successful copying

Watermark protection after Copy Activity failure

Error investigation through ADF Monitor

9. Known Limitations

Timestamp-based loading does not automatically capture hard deletes.

Late-arriving or concurrently committed records may require additional handling.

A failure after copying but before updating the watermark can cause duplicate ingestion on retry.

Stronger data quality validation and idempotent loading would be needed for production use.

10. Future Improvements

SQL Server CDC or Change Tracking

Data validation and quality checks

Idempotent loading

Automated deployment and CI/CD

Monitoring alerts

Databricks and Delta Lake integration
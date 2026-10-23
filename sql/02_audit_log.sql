-- =====================================================================
-- Audit / run log for the metadata-driven pipeline (Azure SQL Database)
--   ctl.pipeline_run_log : one row per notebook run (append-only history)
--   ctl.usp_log_run      : stored procedure that ADF calls after each notebook (Phase 4)
--   ctl.vw_latest_run    : latest attempt per run_date / layer / table
-- The script is re-runnable.
-- =====================================================================

IF OBJECT_ID('ctl.pipeline_run_log', 'U') IS NULL
CREATE TABLE ctl.pipeline_run_log (
    log_id         BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id         VARCHAR(50)   NOT NULL,      -- one id per pipeline run (same for all tables of that run)
    pipeline_name  VARCHAR(100)  NOT NULL,
    layer          VARCHAR(20)   NOT NULL,      -- raw | bronze | silver | gold
    table_name     VARCHAR(100)  NOT NULL,
    run_date       DATE          NULL,          -- the business day that was processed
    status         VARCHAR(20)   NOT NULL,      -- SUCCESS | FAILED | NO_DATA
    rows_read      BIGINT        NULL,
    rows_written   BIGINT        NULL,
    rows_rejected  BIGINT        NULL,          -- sent to quarantine
    rows_deduped   BIGINT        NULL,
    rows_inserted  BIGINT        NULL,
    rows_updated   BIGINT        NULL,
    start_time     DATETIME2     NULL,
    end_time       DATETIME2     NULL,
    duration_sec   AS DATEDIFF(SECOND, start_time, end_time),
    error_message  NVARCHAR(4000) NULL,
    logged_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT ck_run_status CHECK (status IN ('SUCCESS', 'FAILED', 'NO_DATA'))
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = 'ix_run_log_date_table' AND object_id = OBJECT_ID('ctl.pipeline_run_log'))
CREATE INDEX ix_run_log_date_table ON ctl.pipeline_run_log (run_date, layer, table_name);
GO

CREATE OR ALTER PROCEDURE ctl.usp_log_run
    @run_id         VARCHAR(50),
    @pipeline_name  VARCHAR(100),
    @layer          VARCHAR(20),
    @table_name     VARCHAR(100),
    @run_date       DATE           = NULL,
    @status         VARCHAR(20),
    @rows_read      BIGINT         = NULL,
    @rows_written   BIGINT         = NULL,
    @rows_rejected  BIGINT         = NULL,
    @rows_deduped   BIGINT         = NULL,
    @rows_inserted  BIGINT         = NULL,
    @rows_updated   BIGINT         = NULL,
    @start_time     DATETIME2      = NULL,
    @end_time       DATETIME2      = NULL,
    @error_message  NVARCHAR(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO ctl.pipeline_run_log
        (run_id, pipeline_name, layer, table_name, run_date, status,
         rows_read, rows_written, rows_rejected, rows_deduped, rows_inserted, rows_updated,
         start_time, end_time, error_message)
    VALUES
        (@run_id, @pipeline_name, @layer, @table_name, @run_date, @status,
         @rows_read, @rows_written, @rows_rejected, @rows_deduped, @rows_inserted, @rows_updated,
         @start_time, @end_time, @error_message);
END
GO

CREATE OR ALTER VIEW ctl.vw_latest_run AS
SELECT run_date, layer, table_name, status,
       rows_read, rows_written, rows_rejected, rows_deduped, rows_inserted, rows_updated,
       duration_sec, run_id, logged_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY run_date, layer, table_name
                              ORDER BY logged_at DESC, log_id DESC) AS rn
    FROM ctl.pipeline_run_log
) x
WHERE rn = 1;
GO

-- ---------------------------------------------------------------------
-- Quick test (run once, then remove the test rows)
-- ---------------------------------------------------------------------
EXEC ctl.usp_log_run
    @run_id = 'test-001', @pipeline_name = 'pl_test', @layer = 'silver', @table_name = 'orders',
    @run_date = '2018-06-01', @status = 'SUCCESS',
    @rows_read = 385, @rows_written = 385, @rows_rejected = 0, @rows_deduped = 0,
    @rows_inserted = 385, @rows_updated = 0,
    @start_time = '2026-10-22T10:00:00', @end_time = '2026-10-22T10:00:07';

EXEC ctl.usp_log_run
    @run_id = 'test-001', @pipeline_name = 'pl_test', @layer = 'silver', @table_name = 'order_payments',
    @run_date = '2018-06-01', @status = 'FAILED',
    @start_time = '2026-10-22T10:00:08', @end_time = '2026-10-22T10:00:09',
    @error_message = 'Test failure message';

SELECT * FROM ctl.vw_latest_run WHERE run_id = 'test-001';

DELETE FROM ctl.pipeline_run_log WHERE run_id = 'test-001';
GO

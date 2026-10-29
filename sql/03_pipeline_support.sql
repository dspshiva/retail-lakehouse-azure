-- Stored procedures called by the ADF pipelines (re-runnable)

CREATE OR ALTER PROCEDURE ctl.usp_log_from_json
    @run_id        VARCHAR(50),
    @pipeline_name VARCHAR(100),
    @layer         VARCHAR(20),
    @table_name    VARCHAR(100),
    @run_date      VARCHAR(10),
    @result_json   NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @status   VARCHAR(20) = JSON_VALUE(@result_json, '$.status');
    DECLARE @duration INT         = TRY_CAST(JSON_VALUE(@result_json, '$.duration_sec') AS FLOAT);
    DECLARE @end_time DATETIME2   = SYSUTCDATETIME();

    IF @status IS NULL OR @status NOT IN ('SUCCESS', 'NO_DATA')
        SET @status = 'FAILED';          -- unknown or missing status is never reported as success

    INSERT INTO ctl.pipeline_run_log
        (run_id, pipeline_name, layer, table_name, run_date, status,
         rows_read, rows_written, rows_rejected, rows_deduped, rows_inserted, rows_updated,
         start_time, end_time, error_message)
    VALUES
        (@run_id, @pipeline_name, @layer, @table_name, TRY_CAST(@run_date AS DATE), @status,
         TRY_CAST(JSON_VALUE(@result_json, '$.rows_read')     AS BIGINT),
         TRY_CAST(JSON_VALUE(@result_json, '$.rows_written')  AS BIGINT),
         TRY_CAST(JSON_VALUE(@result_json, '$.rows_rejected') AS BIGINT),
         TRY_CAST(JSON_VALUE(@result_json, '$.rows_deduped')  AS BIGINT),
         TRY_CAST(JSON_VALUE(@result_json, '$.rows_inserted') AS BIGINT),
         TRY_CAST(JSON_VALUE(@result_json, '$.rows_updated')  AS BIGINT),
         CASE WHEN @duration IS NULL THEN NULL ELSE DATEADD(SECOND, -@duration, @end_time) END,
         @end_time,
         NULL);
END
GO

CREATE OR ALTER PROCEDURE ctl.usp_log_failure
    @run_id        VARCHAR(50),
    @pipeline_name VARCHAR(100),
    @layer         VARCHAR(20),
    @table_name    VARCHAR(100),
    @run_date      VARCHAR(10),
    @error_message NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO ctl.pipeline_run_log
        (run_id, pipeline_name, layer, table_name, run_date, status, end_time, error_message)
    VALUES
        (@run_id, @pipeline_name, @layer, @table_name, TRY_CAST(@run_date AS DATE), 'FAILED',
         SYSUTCDATETIME(), LEFT(@error_message, 4000));
END
GO

CREATE OR ALTER PROCEDURE ctl.usp_update_watermark
    @config_id   INT,
    @run_date    VARCHAR(10),
    @result_json NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @status VARCHAR(20)  = JSON_VALUE(@result_json, '$.status');
    DECLARE @day    DATE         = TRY_CAST(@run_date AS DATE);
    DECLARE @wm     DATETIME2    = TRY_CAST(NULLIF(JSON_VALUE(@result_json, '$.max_watermark'), '') AS DATETIME2);

    UPDATE ctl.watermark_state
    SET last_ingestion_date  = CASE WHEN @status = 'SUCCESS'
                                     AND (last_ingestion_date IS NULL OR @day > last_ingestion_date)
                                    THEN @day ELSE last_ingestion_date END,
        last_watermark_value = CASE WHEN @status = 'SUCCESS' AND @wm IS NOT NULL
                                     AND (last_watermark_value IS NULL OR @wm > last_watermark_value)
                                    THEN @wm ELSE last_watermark_value END,   -- the watermark never moves backwards
        last_run_status      = @status,
        updated_at           = SYSUTCDATETIME()
    WHERE config_id = @config_id;
END
GO

-- Quick test (the test rows are removed at the end)
DECLARE @json NVARCHAR(MAX) =
 N'{"layer":"silver","table":"orders","status":"SUCCESS","rows_read":385,"rows_rejected":0,"rows_deduped":0,"rows_written":385,"rows_inserted":385,"rows_updated":0,"max_watermark":"2018-06-01 23:59:00","duration_sec":7.4}';

EXEC ctl.usp_log_from_json 'test-002', 'pl_test', 'silver', 'orders', '2018-06-01', @json;
EXEC ctl.usp_log_failure   'test-002', 'pl_test', 'pipeline', 'order_items', '2018-06-01', N'Test error message';

SELECT run_date, layer, table_name, status, rows_read, rows_written, duration_sec
FROM ctl.pipeline_run_log WHERE run_id = 'test-002';

DELETE FROM ctl.pipeline_run_log WHERE run_id = 'test-002';
GO

-- Optional: see the watermark state
SELECT c.table_name, w.last_ingestion_date, w.last_watermark_value, w.last_run_status
FROM ctl.watermark_state w JOIN ctl.pipeline_config c ON c.config_id = w.config_id
ORDER BY c.load_order, c.table_name;
GO
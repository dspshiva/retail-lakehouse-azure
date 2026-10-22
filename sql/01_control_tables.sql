-- ============================================================
-- Control tables for the metadata-driven pipeline (Azure SQL Database)
-- ctl.pipeline_config : static settings per table (changes by deployment)
-- ctl.watermark_state : changing state per table (updated after every run)
-- The script is re-runnable: it creates objects and rows only if missing.
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'ctl'
)
    EXEC('CREATE SCHEMA ctl');
GO

-- 1) STATIC SETTINGS: changes rarely
IF OBJECT_ID('ctl.pipeline_config', 'U') IS NULL
CREATE TABLE ctl.pipeline_config (
    config_id INT IDENTITY(1,1) PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    source_container VARCHAR(50) NOT NULL,
    source_folder VARCHAR(200) NOT NULL,
    raw_container VARCHAR(50) NOT NULL,
    raw_folder VARCHAR(200) NOT NULL,
    file_format VARCHAR(10) NOT NULL,
    load_type VARCHAR(20) NOT NULL, 
    primary_keys VARCHAR(200) NOT NULL, 
    watermark_column VARCHAR(100) NULL, 
    scd_type TINYINT NOT NULL DEFAULT 1,
    load_order INT NOT NULL,
    is_active BIT NOT NULL DEFAULT 1,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT uq_config UNIQUE (source_system, table_name),
    CONSTRAINT ck_load_type
        CHECK (load_type IN ('incremental', 'full'))
);
GO

-- 2) CHANGING STATE: updated after every run
IF OBJECT_ID('ctl.watermark_state', 'U') IS NULL
CREATE TABLE ctl.watermark_state (
    config_id INT PRIMARY KEY
        REFERENCES ctl.pipeline_config(config_id),
    last_ingestion_date DATE NULL,
    last_watermark_value DATETIME2 NULL,
    last_run_status VARCHAR(20) NULL,
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- 3) Config rows (one row per source table)
IF NOT EXISTS (
    SELECT 1 FROM ctl.pipeline_config
)
INSERT INTO ctl.pipeline_config
(
    source_system,
    table_name,
    source_container,
    source_folder,
    raw_container,
    raw_folder,
    file_format,
    load_type,
    primary_keys,
    watermark_column,
    scd_type,
    load_order
)
VALUES
(
    'olist', 'category_translation',
    'source', 'olist/category_translation',
    'raw', 'olist/category_translation',
    'csv', 'full', 'product_category_name',
    NULL, 1, 1
),
(
    'olist', 'geolocation',
    'source', 'olist/geolocation',
    'raw', 'olist/geolocation',
    'csv', 'full', 'geolocation_zip_code_prefix',
    NULL, 1, 1
),
(
    'olist', 'sellers',
    'source', 'olist/sellers',
    'raw', 'olist/sellers',
    'csv', 'full', 'seller_id',
    NULL, 1, 2
),
(
    'olist', 'products',
    'source', 'olist/products',
    'raw', 'olist/products',
    'csv', 'full', 'product_id',
    NULL, 1, 2
),
(
    'olist', 'customers',
    'source', 'olist/customers',
    'raw', 'olist/customers',
    'csv', 'incremental', 'customer_id',
    NULL, 2, 2
),
(
    'olist', 'orders',
    'source', 'olist/orders',
    'raw', 'olist/orders',
    'csv', 'incremental', 'order_id',
    'last_updated_ts', 1, 3
),
(
    'olist', 'order_items',
    'source', 'olist/order_items',
    'raw', 'olist/order_items',
    'csv', 'incremental', 'order_id,order_item_id',
    NULL, 1, 4
),
(
    'olist', 'order_payments',
    'source', 'olist/order_payments',
    'raw', 'olist/order_payments',
    'csv', 'incremental', 'order_id,payment_sequential',
    NULL, 1, 4
),
(
    'olist', 'order_reviews',
    'source', 'olist/order_reviews',
    'raw', 'olist/order_reviews',
    'csv', 'incremental', 'review_id,order_id',
    'review_creation_date', 1, 4
);
GO

-- 4) One state row per config row
INSERT INTO ctl.watermark_state (config_id)
SELECT c.config_id
FROM ctl.pipeline_config c
WHERE NOT EXISTS (
    SELECT 1
    FROM ctl.watermark_state w
    WHERE w.config_id = c.config_id
);
GO

-- 5) Check
SELECT
    table_name,
    load_type,
    primary_keys,
    watermark_column,
    scd_type,
    load_order
FROM ctl.pipeline_config
WHERE is_active = 1
ORDER BY load_order, table_name;
GO

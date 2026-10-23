# Databricks notebook source
import json, uuid, time
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# COMMAND ----------

STORAGE_ACCOUNT = "stretaillakedsp01"
SOURCE_SYSTEM = "olist"

# COMMAND ----------

def path(container, *parts):
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/" + "/".join(parts)


def raw_path(table, run_date):
    return path("raw", SOURCE_SYSTEM, table, f"ingestion_date={run_date}")


def bronze_path(table):
    return path("bronze", SOURCE_SYSTEM, table)


def silver_path(table):
    return path("silver", SOURCE_SYSTEM, table)


def quarantine_path(table):
    return path("silver", "_quarantine", table)


def path_exists(p):
    try:
        dbutils.fs.ls(p)
        return True
    except Exception as e:
        msg = str(e)
        if "FileNotFoundException" in msg or "PATH_NOT_FOUND" in msg or "does not exist" in msg:
            return False
        raise 


# COMMAND ----------




SCHEMAS = {
    "orders": {
        "order_id": "string",
        "customer_id": "string",
        "order_status": "string",
        "order_purchase_timestamp": "timestamp",
        "order_approved_at": "timestamp",
        "order_delivered_carrier_date": "timestamp",
        "order_delivered_customer_date": "timestamp",
        "order_estimated_delivery_date": "timestamp",
        "last_updated_ts": "timestamp",
    },
    "order_items": {
        "order_id": "string",
        "order_item_id": "int",
        "product_id": "string",
        "seller_id": "string",
        "shipping_limit_date": "timestamp",
        "price": "decimal(10,2)",
        "freight_value": "decimal(10,2)",
    },
    "order_payments": {
        "order_id": "string",
        "payment_sequential": "int",
        "payment_type": "string",
        "payment_installments": "int",
        "payment_value": "decimal(10,2)",
    },
    "order_reviews": {
        "review_id": "string",
        "order_id": "string",
        "review_score": "int",
        "review_comment_title": "string",
        "review_comment_message": "string",
        "review_creation_date": "timestamp",
        "review_answer_timestamp": "timestamp",
    },
    "customers": {
        "customer_id": "string",
        "customer_unique_id": "string",
        "customer_zip_code_prefix": "string",
        "customer_city": "string",
        "customer_state": "string",
    },
    "products": {
        "product_id": "string",
        "product_category_name": "string",
        "product_name_lenght": "int",
        "product_description_lenght": "int",
        "product_photos_qty": "int",
        "product_weight_g": "int",
        "product_length_cm": "int",
        "product_height_cm": "int",
        "product_width_cm": "int",
    },
    "sellers": {
        "seller_id": "string",
        "seller_zip_code_prefix": "string",
        "seller_city": "string",
        "seller_state": "string",
    },
    "geolocation": {
        "geolocation_zip_code_prefix": "string",
        "geolocation_lat": "double",
        "geolocation_lng": "double",
        "geolocation_city": "string",
        "geolocation_state": "string",
    },
    "category_translation": {
        "product_category_name": "string",
        "product_category_name_english": "string",
    },
}


# COMMAND ----------



DQ_RULES = {
    "orders": {
        "order_id_not_null": "order_id IS NOT NULL",
        "customer_id_not_null": "customer_id IS NOT NULL",
        "purchase_ts_not_null": "order_purchase_timestamp IS NOT NULL",
        "last_updated_not_null": "last_updated_ts IS NOT NULL",
        "status_valid": (
            "order_status IN "
            "('created','approved','invoiced','processing',"
            "'shipped','delivered','unavailable','canceled')"
        ),
        "delivery_after_purchase": (
            "order_delivered_customer_date IS NULL OR "
            "order_delivered_customer_date >= order_purchase_timestamp"
        ),
    },
    "order_items": {
        "order_id_not_null": "order_id IS NOT NULL",
        "item_no_not_null": "order_item_id IS NOT NULL",
        "product_id_not_null": "product_id IS NOT NULL",
        "seller_id_not_null": "seller_id IS NOT NULL",
        "price_non_negative": "price >= 0",
        "freight_non_negative": "freight_value >= 0",
    },
    "order_payments": {
        "order_id_not_null": "order_id IS NOT NULL",
        "value_non_negative": "payment_value >= 0",
        "seq_not_null": "payment_sequential IS NOT NULL",
        "type_valid": (
            "payment_type IN "
            "('credit_card','boleto','voucher','debit_card','not_defined')"
        ),
    },
    "order_reviews": {
        "review_id_not_null": "review_id IS NOT NULL",
        "order_id_not_null": "order_id IS NOT NULL",
        "creation_date_not_null": "review_creation_date IS NOT NULL",
        "score_1_to_5": "review_score BETWEEN 1 AND 5",
    },
    "customers": {
        "customer_id_not_null": "customer_id IS NOT NULL",
        "unique_id_not_null": "customer_unique_id IS NOT NULL",
        "state_2_letters": "length(customer_state) = 2",
    },
    "products": {
        "product_id_not_null": "product_id IS NOT NULL",
        "weight_non_negative": (
            "product_weight_g IS NULL OR product_weight_g >= 0"
        ),
    },
    "sellers": {
        "seller_id_not_null": "seller_id IS NOT NULL",
        "state_2_letters": "length(seller_state) = 2",
    },
    "geolocation": {
        "zip_not_null": "geolocation_zip_code_prefix IS NOT NULL",
        "lat_range": "geolocation_lat BETWEEN -90 AND 90",
        "lng_range": "geolocation_lng BETWEEN -180 AND 180",
    },
    "category_translation": {
        "pt_name_not_null": "product_category_name IS NOT NULL",
        "en_name_not_null": "product_category_name_english IS NOT NULL",
    },
}

# COMMAND ----------


def t_products(df):
    # fix the spelling mistake in the Olist column names
    return (
        df.withColumnRenamed("product_name_lenght", "product_name_length")
          .withColumnRenamed(
              "product_description_lenght",
              "product_description_length"
          )
    )


def t_geolocation(df):
    return (
        df.groupBy("geolocation_zip_code_prefix")
          .agg(
              F.avg("geolocation_lat").alias("geolocation_lat"),
              F.avg("geolocation_lng").alias("geolocation_lng"),
              F.min("geolocation_city").alias("geolocation_city"),
              F.max("geolocation_state").alias("geolocation_state"),
              F.max("_ingestion_date").alias("_ingestion_date"),
              F.max("_ingested_at").alias("_ingested_at"),
              F.max("_source_file").alias("_source_file"),
          )
    )


# COMMAND ----------


POST_DQ = {
    "products": t_products,
    "geolocation": t_geolocation,
}

print("Common loaded")


# COMMAND ----------

def gold_path(table):
    return path("gold",SOURCE_SYSTEM, table)

LOW_DATE = "1900-01-01"
HIGH_DATE = "9999-12-31"

# COMMAND ----------


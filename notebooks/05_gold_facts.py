# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

# Databricks notebook source
# Gold facts
#   fact_order_items : grain = one row per order item (links to dim_customer, dim_product, dim_seller, dim_date)
#   fact_orders      : grain = one row per order (payments, review, delivery metrics)
# Method  : find the orders that were touched on run_date, rebuild their fact rows from the
#           full silver state, then MERGE (upsert) into the fact tables.
# Widgets : run_date, run_id
# Idempotent: rebuilding the same orders and MERGE on the key gives the same result on every run.
# Run AFTER 04_gold_dimensions for the same run_date.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

dbutils.widgets.text("run_date", "2018-05-31")
dbutils.widgets.text("run_id", "")

run_date = dbutils.widgets.get("run_date")
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())
rd = F.lit(run_date).cast("date")
te = time.time()

result = {
    "layer": "gold",
    "table": "facts",
    "run_date": run_date,
    "run_id": run_id,
    "status": "SUCCESS",
    "rows_read": 0,         # orders touched on this day
    "rows_rejected": 0,
    "rows_written": 0,
    "rows_inserted": 0,     # fact_orders
    "rows_updated": 0,      # fact_orders
    "items_inserted": 0,    # fact_order_items
    "items_updated": 0,     # fact_order_items
}


def silver(t):
    return spark.read.format("delta").load(silver_path(t))


def sk(*cols):
    return F.xxhash64(*[F.col(c) for c in cols])


def dk(c):
    """date -> integer key such as 20180605"""
    return F.date_format(F.col(c), "yyyyMMdd").cast("int")


def add_meta(df):
    return (
        df.withColumn("_gold_run_id", F.lit(run_id))
          .withColumn("_gold_updated_at", F.current_timestamp())
    )


def upsert(df, name, keys):
    """MERGE df into gold table `name`. Returns (inserted, updated)."""
    p = gold_path(name)
    if not DeltaTable.isDeltaTable(spark, p):
        n = df.count()
        df.write.format("delta").save(p)
        return n, 0
    cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    dt = DeltaTable.forPath(spark, p)
    (
        dt.alias("t").merge(df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    m = dt.history(1).select("operationMetrics").collect()[0][0]
    return int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))


# COMMAND ----------

# ---------- 1. Which orders were touched today? ----------
# An order is "touched" when its order row, items, payments or reviews arrived on run_date.
def touched(t):
    return silver(t).filter(F.col("_ingestion_date") == rd).select("order_id")


affected = (
    touched("orders")
    .union(touched("order_items"))
    .union(touched("order_payments"))
    .union(touched("order_reviews"))
    .distinct()
    .cache()
)
result["rows_read"] = affected.count()

if result["rows_read"] == 0:
    result["status"] = "NO_DATA"
    dbutils.notebook.exit(json.dumps(result))

if not DeltaTable.isDeltaTable(spark, gold_path("dim_customer")):
    raise Exception("dim_customer not found. Run 04_gold_dimensions first.")

# COMMAND ----------

# ---------- 2. Orders with the customer version that was valid on the purchase date ----------
dim_c = (
    spark.read.format("delta").load(gold_path("dim_customer"))
    .select(
        "customer_key",
        F.col("customer_id").alias("d_customer_id"),
        "valid_from",
        "valid_to",
    )
)

orders = silver("orders").join(affected, "order_id", "left_semi")
pdate = F.col("order_purchase_timestamp").cast("date")

# point-in-time join: valid_from <= purchase date < valid_to
oe = (
    orders.join(
        dim_c,
        (orders["customer_id"] == dim_c["d_customer_id"])
        & (pdate >= dim_c["valid_from"])
        & (pdate < dim_c["valid_to"]),
        "left",
    )
    .select(
        "order_id",
        F.coalesce("customer_key", F.lit(-1)).alias("customer_key"),   # -1 = unknown customer
        dk("order_purchase_timestamp").alias("purchase_date_key"),
        dk("order_approved_at").alias("approved_date_key"),
        dk("order_delivered_customer_date").alias("delivered_date_key"),
        dk("order_estimated_delivery_date").alias("estimated_date_key"),
        "order_status",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "last_updated_ts",
    )
    .cache()
)

# COMMAND ----------

# ---------- 3. Order-level aggregates ----------
items_agg = (
    silver("order_items").join(affected, "order_id", "left_semi")
    .groupBy("order_id")
    .agg(
        F.count("*").alias("items_count"),
        F.sum("price").alias("items_value"),
        F.sum("freight_value").alias("freight_value"),
    )
)

pay_agg = (
    silver("order_payments").join(affected, "order_id", "left_semi")
    .groupBy("order_id")
    .agg(
        F.sum("payment_value").alias("payment_value"),
        F.max("payment_installments").alias("max_installments"),
        F.expr("max_by(payment_type, payment_value)").alias("main_payment_type"),
    )
)

rev_agg = (
    silver("order_reviews").join(affected, "order_id", "left_semi")
    .groupBy("order_id")
    .agg(F.expr("max_by(review_score, review_creation_date)").alias("review_score"))   # latest review
)

# COMMAND ----------

# ---------- 4. fact_orders (one row per order) ----------
fact_orders = (
    oe.join(items_agg, "order_id", "left")
      .join(pay_agg, "order_id", "left")
      .join(rev_agg, "order_id", "left")
      .select(
          "order_id",
          "customer_key",
          "purchase_date_key",
          "approved_date_key",
          "delivered_date_key",
          "estimated_date_key",
          "order_status",
          F.coalesce("items_count", F.lit(0)).alias("items_count"),
          F.coalesce("items_value", F.lit(0)).alias("items_value"),
          F.coalesce("freight_value", F.lit(0)).alias("freight_value"),
          F.coalesce("payment_value", F.lit(0)).alias("payment_value"),
          "max_installments",
          "main_payment_type",
          "review_score",
          F.datediff("order_delivered_customer_date", "order_purchase_timestamp").alias("delivery_days"),
          F.datediff("order_delivered_customer_date", "order_estimated_delivery_date").alias("delivery_delay_days"),
          F.when(F.col("order_delivered_customer_date").isNull(), F.lit(None).cast("boolean"))
           .otherwise(F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"))
           .alias("is_late"),
          "order_purchase_timestamp",
          "last_updated_ts",
      )
      .transform(add_meta)
)

ins, upd = upsert(fact_orders, "fact_orders", ["order_id"])
result["rows_inserted"], result["rows_updated"] = ins, upd

# COMMAND ----------

# ---------- 5. fact_order_items (one row per order item) ----------
fact_items = (
    silver("order_items").join(affected, "order_id", "left_semi")
    .join(
        oe.select("order_id", "customer_key", "purchase_date_key", "order_status"),
        "order_id",
        "left",
    )
    .select(
        "order_id",
        "order_item_id",
        sk("product_id").alias("product_key"),
        sk("seller_id").alias("seller_key"),
        F.coalesce("customer_key", F.lit(-1)).alias("customer_key"),
        "purchase_date_key",
        "order_status",
        "shipping_limit_date",
        "price",
        "freight_value",
        (F.col("price") + F.col("freight_value")).alias("total_item_value"),
    )
    .transform(add_meta)
)

ins_i, upd_i = upsert(fact_items, "fact_order_items", ["order_id", "order_item_id"])
result["items_inserted"], result["items_updated"] = ins_i, upd_i
result["rows_written"] = ins + upd + ins_i + upd_i

oe.unpersist()
affected.unpersist()

# COMMAND ----------

result["duration_sec"] = round(time.time() - te, 1)
print(result)
dbutils.notebook.exit(json.dumps(result))
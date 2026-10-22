# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

s = spark.read.format("delta").load(silver_path("order_payments"))
print(s.filter("order_id IS NULL OR trim(order_id) = ''").count())

# COMMAND ----------

from datetime import date, timedelta

CONFIG = [  # mirrors ctl.pipeline_config: table, load_type, keys, watermark, load_order
    ("category_translation", "full", "product_category_name", "", 1),
    ("geolocation", "full", "geolocation_zip_code_prefix", "", 1),
    ("sellers", "full", "seller_id", "", 2),
    ("products", "full", "product_id", "", 2),
    ("customers", "incremental", "customer_id", "", 2),
    ("orders", "incremental", "order_id", "last_updated_ts", 3),
    ("order_items", "incremental", "order_id,order_item_id", "", 4),
    ("order_payments", "incremental", "order_id,payment_sequential", "", 4),
    ("order_reviews", "incremental", "review_id,order_id", "review_creation_date", 4),
]


def run_day(run_date):
    run_id, rows = str(uuid.uuid4()), []

    for t, lt, pk, wm, _ in sorted(CONFIG, key=lambda x: x[4]):  # dimensions first
        if not path_exists(raw_path(t, run_date)):
            continue  # nothing arrived today

        a = {
            "table_name": t,
            "run_date": run_date,
            "run_id": run_id,
        }

        b = json.loads(
            dbutils.notebook.run(
                "01_bronze_ingest",
                3600,
                a,
            )
        )

        s = json.loads(
            dbutils.notebook.run(
                "02_silver_transform",
                3600,
                {
                    **a,
                    "load_type": lt,
                    "primary_keys": pk,
                    "watermark_column": wm,
                },
            )
        )

        rows.append(
            (
                t,
                b["rows_read"],
                s["rows_rejected"],
                s["rows_deduped"],
                s["rows_written"],
                s["status"],
            )
        )

    return rows


days = ["2018-05-31"] + [
    (date(2018, 6, 1) + timedelta(days=i)).isoformat()
    for i in range(30)
]

# COMMAND ----------

for r in run_day("2018-05-31"):
    print(r)

# COMMAND ----------

for d in days[1:4]:
    print(d);[print(" ",r) for r in run_day(d)]

# COMMAND ----------

for d in days[4:]:
    print(d);[print(" ",r) for r in run_day(d)]

# COMMAND ----------

# MAGIC %md
# MAGIC Test 1: silver must have no duplication

# COMMAND ----------

o = spark.read.format("delta").load(silver_path("orders"))
print(o.count(),o.select("order_id").distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC Test 2: Reconcile with your files

# COMMAND ----------

# 1) rows and unique orders in the RAW files (what ADF copied)
files = (
    spark.read.format("csv")
    .option("header", True)
    .option("multiline", True)
    .load(path("raw", "olist", "orders", "ingestion_date=*", "*.csv"))
)

print(
    "raw files -> rows:",
    files.count(),
    "| unique orders:",
    files.select("order_id").distinct().count()
)

# 2) rows in Bronze and Silver
b = spark.read.format("delta").load(bronze_path("orders"))
s = spark.read.format("delta").load(silver_path("orders"))

print("bronze rows :", b.count())
print("silver rows :", s.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Test 3: An order that changed status

# COMMAND ----------

b = spark.read.format("delta").load(bronze_path("orders"))

oid = b.groupBy("order_id").count().filter("count > 1").first()["order_id"]

b.filter(F.col("order_id") == oid).select(
    "order_id", "order_status", "last_updated_ts", "_ingestion_date"
).show()

o.filter(F.col("order_id") == oid).select(
    "order_id", "order_status", "last_updated_ts"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Test 4: Idempotency

# COMMAND ----------

n1 = spark.read.format("delta").load(silver_path("orders")).count()

run_day("2018-06-02"); run_day("2018-06-02")  # run the same day again

n2 = spark.read.format("delta").load(silver_path("orders")).count()

print(n1, n2, "OK" if n1 == n2 else "PROBLEM")

# COMMAND ----------

# MAGIC %md
# MAGIC Test 5: Quarantine demo

# COMMAND ----------

bad_csv = """order_id,payment_sequential,payment_type,payment_installments,payment_value
BAD001,1,bitcoin,1,50.00
BAD002,1,credit_card,1,abc
"",1,credit_card,1,10.00
"""

dbutils.fs.put(
    raw_path("order_payments", "2018-06-02") + "/bad_payments.csv",
    bad_csv,
    True
)

run_day("2018-06-02")

q = spark.read.format("delta").load(
    quarantine_path("order_payments")
)

q.select(
    "_dq_failed_rules",
    "_raw_json"
).show(truncate=False)

# COMMAND ----------

display(dbutils.fs.ls(raw_path("order_payments", "2018-06-02")))

# COMMAND ----------

dbutils.fs.rm(raw_path("order_payments", "2018-06-02")+"/bad_payments.csv")

# COMMAND ----------

display(dbutils.fs.ls(raw_path("order_payments", "2018-06-02")))

# COMMAND ----------

for r in run_day("2018-06-02"):
  print(r)

# COMMAND ----------

# MAGIC %md
# MAGIC Check that the cleanup worked

# COMMAND ----------

q = spark.read.format("delta").load(
    quarantine_path("order_payments")
)
print("quarantine rows for 2018-06-02:", q.filter("_ingestion_date = '2018-06-02'").count())

for name, p in [("bronze", bronze_path("order_payments")), ("silver", silver_path("order_payments"))]:
    n = spark.read.format("delta").load(p).filter("order_id LIKE 'BAD%'").count()
    print(name, "BAD rows:",n)

# COMMAND ----------

print(spark.read.format("delta").load(silver_path("order_payments")).filter("order_id IS NULL OR trim(order_id) = ''").count())

# COMMAND ----------


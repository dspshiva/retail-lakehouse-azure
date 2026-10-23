# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

dbutils.notebook.run("04_gold_dimensions",3600,{"run_date":"2018-05-31"})

# COMMAND ----------

display(dbutils.fs.ls(path("gold","olist")))

# COMMAND ----------

dbutils.notebook.run("05_gold_facts",3600,{"run_date":"2018-05-31"})

# COMMAND ----------

fo = spark.read.format("delta").load(gold_path("fact_orders"))
fi = spark.read.format("delta").load(gold_path("fact_order_items"))
print("fact_orders:", fo.count(), "| distinct orders:", fo.select("order_id").distinct().count())
print("fact_order_items:", fi.count())
print("orders with unknown customer (-1):", fo.filter("customer_key = -1").count())
fo.show(3, truncate=False)

# COMMAND ----------

from datetime import date, timedelta

days = ["2018-05-31"] + [(date(2018, 6, 1) + timedelta(days=i)).isoformat() for i in range(30)]

def run_gold_day(run_date):
    a = {"run_date": run_date, "run_id": str(uuid.uuid4())}
    d = json.loads(dbutils.notebook.run("04_gold_dimensions", 3600, a))   # dimensions first
    f = json.loads(dbutils.notebook.run("05_gold_facts", 3600, a))        # then facts
    return {
        "date": run_date,
        "dims": d["built"], "cust_inserted": d["rows_inserted"], "cust_closed": d["rows_updated"],
        "facts_status": f["status"], "orders_touched": f["rows_read"],
        "orders_ins": f["rows_inserted"], "orders_upd": f["rows_updated"],
        "items_ins": f["items_inserted"], "items_upd": f["items_updated"],
    }

# COMMAND ----------

for d in days[1:4]:
    print(run_gold_day(d))

# COMMAND ----------

for d in days[4:]:
    print(run_gold_day(d))

# COMMAND ----------

def counts():
    return {t: spark.read.format("delta").load(gold_path(t)).count()
            for t in ["dim_customer", "fact_orders", "fact_order_items"]}

before = counts()
print(run_gold_day("2018-06-30"))
after = counts()
print(before)
print(after)
print("OK" if before == after else "PROBLEM")

# COMMAND ----------


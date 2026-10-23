# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

# Databricks notebook source
# Gold validation tests. These cells only READ data, so "Run all" is safe.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

def gold(name):
    return spark.read.format("delta").load(gold_path(name))


def silver(t):
    return spark.read.format("delta").load(silver_path(t))


def check(name, ok, detail=""):
    print(("OK       " if ok else "PROBLEM  ") + name, detail)


# COMMAND ----------

# MAGIC %md
# MAGIC Test 1: dim_customer SCD2 rules

# COMMAND ----------

dc = gold("dim_customer")

total_customers = dc.select("customer_id").distinct().count()
current_rows = dc.filter("is_current = true").count()
two_current = dc.filter("is_current = true").groupBy("customer_id").count().filter("count > 1").count()
dup_keys = dc.groupBy("customer_key").count().filter("count > 1").count()

check("one current version per customer", current_rows == total_customers and two_current == 0,
      f"(customers={total_customers}, current={current_rows}, with 2 current={two_current})")
check("customer_key is unique", dup_keys == 0, f"(duplicates={dup_keys})")

changed = dc.groupBy("customer_id").count().filter("count > 1")
print("customers with more than one version:", changed.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Test 2: history of customers who moved (take a screenshot of this one)

# COMMAND ----------

(
    dc.join(changed, "customer_id")
    .orderBy("customer_id", "valid_from")
    .select("customer_id", "customer_city", "customer_state", "valid_from", "valid_to", "is_current")
    .show(10, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Test 3: fact tables vs silver

# COMMAND ----------

fo = gold("fact_orders")
fi = gold("fact_order_items")

n_fo = fo.count()
n_fo_distinct = fo.select("order_id").distinct().count()
n_silver_orders = silver("orders").count()
check("fact_orders: no duplicate orders", n_fo == n_fo_distinct, f"(rows={n_fo}, distinct={n_fo_distinct})")
check("fact_orders count = silver orders count", n_fo == n_silver_orders, f"(gold={n_fo}, silver={n_silver_orders})")

n_fi = fi.count()
n_fi_distinct = fi.select("order_id", "order_item_id").distinct().count()
n_silver_items = silver("order_items").count()
check("fact_order_items: no duplicate items", n_fi == n_fi_distinct, f"(rows={n_fi}, distinct={n_fi_distinct})")
check("fact_order_items count = silver order_items count", n_fi == n_silver_items, f"(gold={n_fi}, silver={n_silver_items})")

# COMMAND ----------

# MAGIC %md
# MAGIC Test 4: money reconciles

# COMMAND ----------

g_price = fi.agg(F.sum("price")).collect()[0][0]
s_price = silver("order_items").agg(F.sum("price")).collect()[0][0]
check("sum(price): gold = silver", g_price == s_price, f"(gold={g_price}, silver={s_price})")

g_pay = fo.agg(F.sum("payment_value")).collect()[0][0]
s_pay = silver("order_payments").agg(F.sum("payment_value")).collect()[0][0]
check("sum(payment_value): gold = silver", g_pay == s_pay, f"(gold={g_pay}, silver={s_pay})")

# COMMAND ----------

# MAGIC %md
# MAGIC Test 5: no orphan keys (every fact key finds its dimension row)

# COMMAND ----------

dim_p = gold("dim_product").select("product_key")
dim_s = gold("dim_seller").select("seller_key")
dim_d = gold("dim_date").select(F.col("date_key").alias("purchase_date_key"))
dim_c = gold("dim_customer").select("customer_key")

orph_p = fi.join(dim_p, "product_key", "left_anti").count()
orph_s = fi.join(dim_s, "seller_key", "left_anti").count()
orph_d = fi.join(dim_d, "purchase_date_key", "left_anti").count()
orph_c = fo.join(dim_c, "customer_key", "left_anti").count()

check("items -> dim_product", orph_p == 0, f"(orphans={orph_p})")
check("items -> dim_seller", orph_s == 0, f"(orphans={orph_s})")
check("items -> dim_date", orph_d == 0, f"(orphans={orph_d})")
check("orders -> dim_customer", orph_c == 0, f"(orphans={orph_c}, -1 means unknown customer)")

# COMMAND ----------

# MAGIC %md
# MAGIC Test 6: point-in-time proof (an order shows the address that was valid when it was bought)

# COMMAND ----------

moved = changed.select("customer_id")

(
    fo.join(dc.select("customer_key", "customer_id", "customer_city", "valid_from", "valid_to", "is_current"),
            "customer_key")
    .join(moved, "customer_id", "left_semi")
    .select("order_id", "customer_id", "order_purchase_timestamp", "customer_city",
            "valid_from", "valid_to", "is_current")
    .orderBy("customer_id", "order_purchase_timestamp")
    .show(10, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Test 7: quick business view

# COMMAND ----------

display(spark.read.format("delta").load(gold_path("agg_monthly_category"))
        .orderBy(F.desc("revenue")).limit(10))
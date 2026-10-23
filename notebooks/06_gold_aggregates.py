# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

# Databricks notebook source
# Gold business aggregates (built from the star schema, ready for Power BI)
#   agg_daily_sales      : orders, revenue, delivery performance per purchase day
#   agg_monthly_category : items sold and revenue per month and product category
# Both tables are rebuilt completely (small tables, always consistent with the facts).

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

def gold(name):
    return spark.read.format("delta").load(gold_path(name))


def save(df, name):
    df = df.withColumn("_gold_updated_at", F.current_timestamp())
    (
        df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path(name))
    )
    print(name, "->", df.count(), "rows")


dim_date = gold("dim_date")

# COMMAND ----------

# ---------- agg_daily_sales ----------
fo = gold("fact_orders")

agg_daily_sales = (
    fo.groupBy("purchase_date_key")
    .agg(
        F.count("*").alias("orders"),
        F.sum("items_value").alias("items_value"),
        F.sum("freight_value").alias("freight_value"),
        F.sum("payment_value").alias("payment_value"),
        F.sum(F.when(F.col("order_status") == "delivered", 1).otherwise(0)).alias("delivered_orders"),
        F.sum(F.when(F.col("is_late"), 1).otherwise(0)).alias("late_orders"),
        F.avg("delivery_days").alias("avg_delivery_days"),
        F.avg("review_score").alias("avg_review_score"),
    )
    .join(
        dim_date.select(
            F.col("date_key").alias("purchase_date_key"),
            F.col("date").alias("order_date"),
            "year",
            "month",
        ),
        "purchase_date_key",
        "left",
    )
)
save(agg_daily_sales, "agg_daily_sales")

# COMMAND ----------

# ---------- agg_monthly_category ----------
fi = gold("fact_order_items")
dp = gold("dim_product").select("product_key", "category_english")

agg_monthly_category = (
    fi.join(dp, "product_key", "left")
    .join(
        dim_date.select(F.col("date_key").alias("purchase_date_key"), "year", "month"),
        "purchase_date_key",
        "left",
    )
    .groupBy("year", "month", F.coalesce("category_english", F.lit("unknown")).alias("category"))
    .agg(
        F.count("*").alias("items_sold"),
        F.sum("price").alias("revenue"),
        F.sum("freight_value").alias("freight"),
    )
)
save(agg_monthly_category, "agg_monthly_category")

# COMMAND ----------

display(spark.read.format("delta").load(gold_path("agg_daily_sales")).orderBy("order_date").limit(10))
# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

# Databricks notebook source
# Gold dimensions
#   dim_date, dim_product, dim_seller : SCD Type 1 (rebuilt when the silver source receives new data)
#   dim_customer                      : SCD Type 2 (history kept with valid_from / valid_to / is_current)
# Widgets : run_date, run_id
# Output  : JSON with counts for the audit log
# Idempotent: running the same run_date again creates no new versions and no duplicates.
# Run the days IN ORDER (2018-05-31 first), because SCD2 compares against the current version.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

from functools import reduce

dbutils.widgets.text("run_date", "2018-05-31")
dbutils.widgets.text("run_id", "")

run_date = dbutils.widgets.get("run_date")
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())
rd = F.lit(run_date).cast("date")
te = time.time()

result = {
    "layer": "gold",
    "table": "dimensions",
    "run_date": run_date,
    "run_id": run_id,
    "status": "SUCCESS",
    "rows_read": 0,
    "rows_rejected": 0,
    "rows_written": 0,      # rows written by the SCD1 dimensions
    "rows_inserted": 0,     # new SCD2 versions in dim_customer
    "rows_updated": 0,      # SCD2 versions closed (expired) in dim_customer
    "built": [],
}


def silver(t):
    return spark.read.format("delta").load(silver_path(t))


def has_new(t):
    """True if the silver table received rows on run_date."""
    return silver(t).filter(F.col("_ingestion_date") == rd).limit(1).count() > 0


def sk(*cols):
    """Surrogate key: a hash of the business key. Same input gives the same key on every run."""
    return F.xxhash64(*[F.col(c) for c in cols])


def add_meta(df):
    return (
        df.withColumn("_gold_run_id", F.lit(run_id))
          .withColumn("_gold_updated_at", F.current_timestamp())
    )


def write_overwrite(df, name):
    """SCD1 dimension: replace the whole table (small tables, safe to rebuild)."""
    df = add_meta(df)
    n = df.count()
    (
        df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path(name))
    )
    result["rows_written"] += n
    result["built"].append(f"{name}: {n} rows")


# COMMAND ----------

# ---------- dim_date (built once) ----------
if not DeltaTable.isDeltaTable(spark, gold_path("dim_date")):
    d = spark.sql(
        "SELECT explode(sequence(to_date('2016-01-01'), to_date('2019-12-31'), interval 1 day)) AS date"
    )
    d = d.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),       # 1 = Sunday
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )
    write_overwrite(d, "dim_date")

# COMMAND ----------

# ---------- dim_product (SCD1) ----------
if (
    not DeltaTable.isDeltaTable(spark, gold_path("dim_product"))
    or has_new("products")
    or has_new("category_translation")
):
    dim_product = (
        silver("products")
        .join(silver("category_translation"), "product_category_name", "left")
        .select(
            sk("product_id").alias("product_key"),
            "product_id",
            "product_category_name",
            F.coalesce(
                "product_category_name_english", "product_category_name", F.lit("unknown")
            ).alias("category_english"),
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        )
    )
    write_overwrite(dim_product, "dim_product")

# COMMAND ----------

# ---------- dim_seller (SCD1) ----------
if (
    not DeltaTable.isDeltaTable(spark, gold_path("dim_seller"))
    or has_new("sellers")
    or has_new("geolocation")
):
    geo = silver("geolocation").select(
        F.col("geolocation_zip_code_prefix").alias("seller_zip_code_prefix"),
        F.col("geolocation_lat").alias("seller_lat"),
        F.col("geolocation_lng").alias("seller_lng"),
    )
    dim_seller = (
        silver("sellers")
        .join(geo, "seller_zip_code_prefix", "left")      # geolocation is unique per zip, so no row multiplication
        .select(
            sk("seller_id").alias("seller_key"),
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
            "seller_lat",
            "seller_lng",
        )
    )
    write_overwrite(dim_seller, "dim_seller")

# COMMAND ----------

# ---------- dim_customer (SCD Type 2) ----------
# Source = BRONZE (it keeps every version that arrived). Silver keeps only the latest state,
# so it cannot show what the address was before. Silver is used only as a quality gate:
# a customer_id must exist in silver (it passed the data quality rules).
TRACKED = ["customer_zip_code_prefix", "customer_city", "customer_state"]   # a change here creates a new version
ATTRS = ["customer_id", "customer_unique_id"] + TRACKED
dim_path = gold_path("dim_customer")

bronze_day = (
    spark.read.format("delta").load(bronze_path("customers"))
    .filter(F.col("_ingestion_date") == rd)
)

src = (
    bronze_day.select(*[F.expr(f"nullif(trim(`{c}`), '')").alias(c) for c in ATTRS])
    .join(silver("customers").select("customer_id"), "customer_id", "left_semi")
    .dropDuplicates(["customer_id"])
    .cache()
)
n_src = src.count()
result["rows_read"] += n_src


def with_versions(df, valid_from):
    return (
        df.withColumn("valid_from", valid_from)
          .withColumn("valid_to", F.lit(HIGH_DATE).cast("date"))
          .withColumn("is_current", F.lit(True))
          .withColumn("customer_key", sk("customer_id", "valid_from"))   # one key per version
          .transform(add_meta)
    )


LOW = F.lit(LOW_DATE).cast("date")

if n_src == 0:
    result["built"].append("dim_customer: no customer rows on this day")

elif not DeltaTable.isDeltaTable(spark, dim_path):
    # first load: every customer gets one version, valid from "the beginning of time"
    with_versions(src, LOW).write.format("delta").save(dim_path)
    result["rows_inserted"] += n_src
    result["built"].append(f"dim_customer: initial load {n_src} rows")

else:
    dim = DeltaTable.forPath(spark, dim_path)

    cur = (
        dim.toDF().filter("is_current = true")
        .select(
            "customer_id",
            F.lit(True).alias("_found"),
            *[F.col(c).alias(f"cur_{c}") for c in TRACKED],
        )
    )
    j = src.join(cur, "customer_id", "left")
    changed = reduce(
        lambda a, b: a | b,
        [~F.col(c).eqNullSafe(F.col(f"cur_{c}")) for c in TRACKED],     # null-safe comparison
    )

    new_rows = with_versions(j.filter(F.col("_found").isNull()).select(*src.columns), LOW)
    chg_rows = with_versions(j.filter(F.col("_found") & changed).select(*src.columns), rd)

    # count BEFORE the merge (after the merge the changes would already be applied)
    n_new = new_rows.count()
    n_chg = chg_rows.count()

    # Staging trick:
    #  - rows with merge_key = customer_id  -> MATCH the current version -> it is closed (expired)
    #  - rows with merge_key = NULL         -> never match              -> inserted as new version
    to_expire = chg_rows.withColumn("merge_key", F.col("customer_id"))
    to_insert = (
        new_rows.unionByName(chg_rows)
        .withColumn("merge_key", F.lit(None).cast("string"))
    )
    staged = to_expire.unionByName(to_insert)

    cols = dim.toDF().columns
    (
        dim.alias("t")
        .merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(set={
            "is_current": "false",
            "valid_to": "s.valid_from",          # old version ends where the new one starts
            "_gold_run_id": "s._gold_run_id",
            "_gold_updated_at": "s._gold_updated_at",
        })
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in cols})
        .execute()
    )

    result["rows_inserted"] += n_new + n_chg
    result["rows_updated"] += n_chg
    result["built"].append(f"dim_customer: {n_new} new, {n_chg} changed")

src.unpersist()

# COMMAND ----------

result["duration_sec"] = round(time.time() - te, 1)
print(result)
dbutils.notebook.exit(json.dumps(result))
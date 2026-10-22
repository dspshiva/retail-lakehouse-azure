# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

dbutils.widgets.text("table_name", "products")
dbutils.widgets.text("load_type", "full")
dbutils.widgets.text("primary_keys", "product_id")
dbutils.widgets.text("watermark_column", "")
dbutils.widgets.text("run_date", "2018-05-31")
dbutils.widgets.text("run_id", "")

table_name = dbutils.widgets.get("table_name")
load_type = dbutils.widgets.get("load_type")
keys = [k.strip() for k in dbutils.widgets.get("primary_keys").split(",")]
wm = dbutils.widgets.get("watermark_column").strip()
wm = None if wm.lower() in ("", "null", "none") else wm
run_date = dbutils.widgets.get("run_date")
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())

te = time.time()

result = {
    "layer": "silver",
    "table": table_name,
    "run_date": run_date,
    "run_id": run_id,
    "status": "SUCCESS",
    "rows_read": 0,
    "rows_written": 0,
    "rows_rejected": 0,
    "rows_deduped": 0,
    "rows_inserted": 0,
    "rows_updated": 0,
    "max_watermark": None,
}

if not DeltaTable.isDeltaTable(spark, bronze_path(table_name)):
    result["status"] = "NO_DATA"
    dbutils.notebook.exit(json.dumps(result))

schema = SCHEMAS[table_name]
raw = (
    spark.read.format("delta")
    .load(bronze_path(table_name))
    .filter(F.col("_ingestion_date") == F.lit(run_date).cast("date"))
)

result["rows_read"] = raw.count()
if result["rows_read"] == 0:
    result["status"] = "NO_DATA"
    dbutils.notebook.exit(json.dumps(result))

raw = raw.withColumn(
    "_raw_json",
    F.to_json(F.struct(*[F.col(c) for c in schema]))
)

bad_cast = [
    F.when(
        F.col(c).isNotNull()
        & F.expr(f"try_cast(`{c}` AS {t})").isNull(),
        F.lit(f"cast_failed:{c}")
    )
    for c, t in schema.items()
    if t != "string"
]

cast_failed = (
    F.filter(F.array(*bad_cast), lambda x: x.isNotNull())
    if bad_cast
    else F.array().cast("array<string>")
)

typed = raw.select(
    *[
        (
            F.expr(f"nullif(trim({c}), '')")
            if t == "string"
            else F.expr(f"try_cast({c} AS {t})")
        ).alias(c)
        for c, t in schema.items()
    ],
    "_ingestion_date",
    "_ingested_at",
    "_source_file",
    "_raw_json",
    cast_failed.alias("_cast_failed"),
)

rule_fail = [
    F.when(~F.coalesce(F.expr(f"({e})"), F.lit(False)), F.lit(n))
    for n, e in DQ_RULES[table_name].items()
]

typed = (
    typed.withColumn(
        "_dq_failed_rules",
        F.array_union("_cast_failed",
                      F.filter(F.array(*rule_fail), lambda x: x.isNotNull()))
    )
.drop("_cast_failed")
.cache()
)
good = (
    typed.filter(F.size("_dq_failed_rules") == 0)
    .drop("_dq_failed_rules", "_raw_json")
)

bad = typed.filter(
    (F.size("_dq_failed_rules") > 0)
)

bad_out = (
    bad.withColumn("_table_name", F.lit(table_name))
    .withColumn("_silver_run_id", F.lit(run_id))
    .withColumn("_quarantined_at", F.current_timestamp())
)

(
    bad_out.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"_ingestion_date = '{run_date}'")
    .option("mergeSchema", "true")
    .save(quarantine_path(table_name))
)

result["rows_rejected"] = bad.count()
good_cnt = good.count()

if table_name in POST_DQ:
    good = POST_DQ[table_name](good)

order_cols = (
    ([F.col(wm).desc()] if wm else [])
    + [F.col("_source_file").desc()]
)

w = Window.partitionBy(*keys).orderBy(*order_cols)

good = (
    good.withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn")
)

good = (
    good.withColumn("_silver_run_id", F.lit(run_id))
    .withColumn("_silver_updated_at", F.current_timestamp())
)

result["rows_written"] = good.count()
result["rows_deduped"] = good_cnt - result["rows_written"]

if wm:
    mx = good.agg(F.max(wm)).collect()[0][0]
    result["max_watermark"] = str(mx) if mx else None

tgt = silver_path(table_name)

if load_type == "full":
    good.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(tgt)
    result["rows_inserted"] = int(result["rows_written"])

elif not DeltaTable.isDeltaTable(spark, tgt):
    good.write.format("delta").save(tgt)
    result["rows_inserted"] = int(result["rows_written"])

else:
    cond = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys])
    upd = f"s.`{wm}` >= t.`{wm}`" if wm else None

    dt = DeltaTable.forPath(spark, tgt)
    merge = dt.alias("t").merge(good.alias("s"), cond)

    if upd:
        merge = merge.whenMatchedUpdateAll(condition=upd)
    else:
        merge = merge.whenMatchedUpdateAll()

    (
        merge.whenNotMatchedInsertAll()
        .execute()
    )

    m = dt.history(1).select("operationMetrics").collect()[0][0]
    result["rows_inserted"] = int(m.get("numTargetRowsInserted", 0))
    result["rows_updated"] = int(m.get("numTargetRowsUpdated", 0))

typed.unpersist()
result["duration_sec"] = round(time.time() - te, 1)
print(result)
dbutils.notebook.exit(json.dumps(result))

# COMMAND ----------


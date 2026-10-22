# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

dbutils.widgets.text("table_name", "orders")
dbutils.widgets.text("run_date", "2018-06-01")
dbutils.widgets.text("run_id", "")

table_name = dbutils.widgets.get("table_name")
run_date = dbutils.widgets.get("run_date")
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())

te = time.time()

result = {
    "layer": "bronze",
    "table": table_name,
    "run_date": run_date,
    "run_id": run_id,
    "status": "SUCCESS",
    "rows_read": 0,
    "rows_written": 0,
}

src = raw_path(table_name, run_date)
if not path_exists(src):

    result["status"] = "NO_DATA"
    dbutils.notebook.exit(json.dumps(result))

df = (
    spark.read.format("csv")
    .option("header", True)
    .option("multiline", True)
    .option("quote", '"') 
    .option("escape", '"')
    .option("inferSchema", False) 
    .load(src + "/*.csv")
    .select("*", F.col("_metadata.file_path").alias("_source_file"))
)

df = df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])

df = (
    df.withColumn("_ingestion_date", F.lit(run_date).cast("date"))
      .withColumn("_ingested_at", F.current_timestamp())
      .withColumn("_run_id", F.lit(run_id))
)

result["rows_read"] = df.count()

(
    df.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"_ingestion_date = '{run_date}'")
    .option("mergeSchema", "true")
    .partitionBy("_ingestion_date")
    .save(bronze_path(table_name))
)

result["rows_written"] = result["rows_read"]
result["duration_sec"] = round(time.time() - te, 1)
print(result)
dbutils.notebook.exit(json.dumps(result))


# COMMAND ----------


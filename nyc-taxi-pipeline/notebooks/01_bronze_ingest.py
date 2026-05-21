# notebooks/01_bronze_ingest.py
from pyspark.sql.functions import current_timestamp, lit

dbutils.widgets.text("load_type", "incremental") 
dbutils.widgets.text("year", "2026")
dbutils.widgets.text("month", "01")

load_type = dbutils.widgets.get("load_type")
year = dbutils.widgets.get("year")
month = f"{int(dbutils.widgets.get('month')):02d}"

spark.sql("CREATE DATABASE IF NOT EXISTS nyc_taxi_medallion")

if load_type == "base":
    print("Initial Load")
    source_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-12.parquet"
else:
    print(f"Incremental Load for {year}-{month}")
    source_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"

df_raw = spark.read.parquet(source_url)

df_bronze = df_raw.withColumn("ingest_timestamp", current_timestamp()) \
                  .withColumn("source_year", lit(year)) \
                  .withColumn("source_month", lit(month))

df_bronze.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("nyc_taxi_medallion.bronze_yellow_trips")

print("Bronze data successfully ingested!")
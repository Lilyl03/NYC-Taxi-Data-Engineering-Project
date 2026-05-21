from pyspark.sql.functions import col, unix_timestamp
from delta.tables import DeltaTable

dbutils.widgets.text("year", "2026")
dbutils.widgets.text("month", "01")
year = dbutils.widgets.get("year")
month = f"{int(dbutils.widgets.get('month')):02d}"

df_bronze_batch = spark.read.table("nyc_taxi_medallion.bronze_yellow_trips") \
    .filter((col("source_year") == year) & (col("source_month") == month))

df_silver_batch = df_bronze_batch.filter(
    (col("trip_distance") > 0) & 
    (col("passenger_count") > 0) & 
    (col("total_amount") > 0)
).withColumn(
    "trip_duration_minutes", 
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
).filter(
    (col("trip_duration_minutes") > 0) & (col("trip_duration_minutes") < 180)
)

if not spark.catalog.tableExists("nyc_taxi_medallion.silver_yellow_trips"):
    df_silver_batch.write.format("delta").saveAsTable("nyc_taxi_medallion.silver_yellow_trips")
    print("Silver table created and base data loaded!")
else:
    target_table = DeltaTable.forName(spark, "nyc_taxi_medallion.silver_yellow_trips")
    
    target_table.alias("target").merge(
        source = df_silver_batch.alias("updates"),
        condition = "target.VendorID = updates.VendorID AND target.tpep_pickup_datetime = updates.tpep_pickup_datetime"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
     
    print(f"Incremental batch for {year}-{month} merged into Silver successfully!")
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, lit
import mlflow
import mlflow.spark


dbutils.widgets.text("year", "2026")
dbutils.widgets.text("month", "01")
year = dbutils.widgets.get("year")
month = f"{int(dbutils.widgets.get('month')):02d}"

df_silver_batch = spark.read.table("nyc_taxi_medallion.silver_yellow_trips") \
    .filter((col("source_year") == year) & (col("source_month") == month))

feature_cols = ["trip_distance", "passenger_count", "trip_duration_minutes"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

lr = LinearRegression(featuresCol="features", labelCol="total_amount")

pipeline = Pipeline(stages=[assembler, lr])

with mlflow.start_run(run_name=f"Taxi_ML_Batch_{year}_{month}") as run:
    
    model = pipeline.fit(df_silver_batch)
    
    predictions = model.transform(df_silver_batch)
    
    mlflow.spark.log_model(model, "spark_linear_reg_model")
    
    df_gold = predictions.select(
        "VendorID", 
        "tpep_pickup_datetime", 
        "total_amount", 
        "prediction"
    ).withColumn("batch_year", lit(year)) \
     .withColumn("batch_month", lit(month))
    
    df_gold.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("nyc_taxi_medallion.gold_predictions")
        
    print(f"Model logged and predictions saved to Gold for {year}-{month}!")
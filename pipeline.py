from pyspark.sql import SparkSession
import os

# Set environment
print("HADOOP_HOME:", os.environ.get("HADOOP_HOME"))
print("Winutils exists:", os.path.exists(os.path.join(os.environ.get("HADOOP_HOME", ""), "bin", "winutils.exe")))

# Start Spark session (disable native Hadoop I/O)
spark = SparkSession.builder \
    .appName("NYC Yellow Taxi Analysis") \
    .config("spark.hadoop.hadoop.native.lib", "false") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.driver.extraJavaOptions", "-Djava.library.path=") \
    .config("spark.executor.extraJavaOptions", "-Djava.library.path=") \
    .getOrCreate()


# Input file
input_path = "C:/Mini_AI_project/Data Pipeline project/Ollama_Test/Data/yellow_tripdata_2022-01.parquet"
df = spark.read.parquet(input_path)

# Compute fare per mile
df = df.withColumn("fare_per_mile", df.fare_amount / df.trip_distance)

# Group and aggregate
df_result = df.groupBy("passenger_count").avg("fare_per_mile").orderBy("passenger_count")

# Show and save
print("Row count before write:", df_result.count())
df_result.show(truncate=False)

# Output as CSV (instead of Parquet to avoid native IO)
output_path = "C:/Mini_AI_project/Data Pipeline project/output/output.csv"
df_result.toPandas().to_csv(output_path, index=False)
print(f"Output saved to {output_path}")

# Done
spark.stop()

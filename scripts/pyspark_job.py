# scripts/spark_job.py

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SampleSparkJob") \
    .getOrCreate()

# Sample data creation
data = [("Alice", 28), ("Bob", 35), ("Cathy", 22)]
columns = ["Name", "Age"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show data
df.show()

# Stop Spark session
spark.stop()

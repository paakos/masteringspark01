# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

# Initialize Spark session
spark = SparkSession.builder.appName("MergeRows").getOrCreate()

# Sample data
data = [
    (1, "A", 100),
    (2, "B", 200),
    (3, "C", 300),
    (4, "D", 400)
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "value"])

# New data to merge
new_data = [
    (1, "A", 150),  # Update existing row
    (5, "E", 500)   # New row
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create DataFrame for new data
new_df = spark.createDataFrame(new_data, ["id", "name", "value"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge logic
merged_df = df.join(new_df, on="id", how="outer") \
    .select(
        col("id"),
        when(col("new_df.name").isNotNull(), col("new_df.name")).otherwise(col("df.name")).alias("name"),
        when(col("new_df.value").isNotNull(), col("new_df.value")).otherwise(col("df.value")).alias("value"),
        when(col("new_df.id").isNotNull(), lit("new")).otherwise(lit("old")).alias("status")
    )

# Show result
merged_df.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

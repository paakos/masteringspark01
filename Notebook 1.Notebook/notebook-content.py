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
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

# Initialize Spark session
spark = SparkSession.builder.appName("MergeRows").getOrCreate()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Sample data
data = [
    (1, "A", 100),
    (2, "B", 200),
    (3, "C", 300),
    (4, "D", 400)
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "value"])

df.show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# New data to merge
new_data = [
    (1, "A", 150),  # Update existing row
    (5, "E", 500)   # New row
]

# Create DataFrame for new data
new_df = spark.createDataFrame(new_data, ["id", "name", "value"])

new_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge logic
merged_df = df_alias.join(new_df_alias, df_alias["id"] == new_df_alias["id"], how="outer") \
    .select(
        df_alias["id"],
        when(new_df_alias["name"].isNotNull(), new_df_alias["name"]).otherwise(df_alias["name"]).alias("name"),
        when(new_df_alias["value"].isNotNull(), new_df_alias["value"]).otherwise(df_alias["value"]).alias("value"),
        when(new_df_alias["id"].isNotNull(), lit("new")).otherwise(lit("updated")).alias("status")
    )

# Show result
merged_df.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Assuming df and new_df are already defined DataFrames with an 'id' column
# Alias the DataFrames to avoid column name ambiguity
df_alias = df.alias("df")
new_df_alias = new_df.alias("new_df")

merged_df = df_alias.join(new_df_alias, df_alias["id"] == new_df_alias["id"], how="outer") \
    .select(
        df_alias["id"],
        when(new_df_alias["name"].isNotNull(), new_df_alias["name"]).otherwise(df_alias["name"]).alias("name"),
        when(new_df_alias["value"].isNotNull(), new_df_alias["value"]).otherwise(df_alias["value"]).alias("value"),
        when(new_df_alias["id"].isNotNull() & df_alias["id"].isNull(), lit("new"))
        .when(new_df_alias["id"].isNotNull() & df_alias["id"].isNotNull(), lit("updated"))
        .otherwise(lit("old")).alias("status")
    )

# Show the result
merged_df.show()

# Note: Replace 'df' and 'new_df' with the actual DataFrame variable names.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Assuming df and new_df are already defined DataFrames with an 'id' column
# Alias the DataFrames to avoid column name ambiguity
df_alias = df.alias("df")
new_df_alias = new_df.alias("new_df")

# Modify the select statement to insert the new id when the row is new
merged_df = df_alias.join(new_df_alias, df_alias["id"] == new_df_alias["id"], how="outer") \
    .select(
        when(new_df_alias["id"].isNotNull() & df_alias["id"].isNull(), new_df_alias["id"]).otherwise(df_alias["id"]).alias("id"),
        when(new_df_alias["name"].isNotNull(), new_df_alias["name"]).otherwise(df_alias["name"]).alias("name"),
        when(new_df_alias["value"].isNotNull(), new_df_alias["value"]).otherwise(df_alias["value"]).alias("value"),
        when(new_df_alias["id"].isNotNull() & df_alias["id"].isNull(), lit("new"))
        .when(new_df_alias["id"].isNotNull() & df_alias["id"].isNotNull(), lit("updated"))
        .otherwise(lit("old")).alias("status")
    )

# Show the result
merged_df.show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Assuming df and new_df are already defined DataFrames with an 'id' column
df.createOrReplaceTempView("df")
new_df.createOrReplaceTempView("new_df")

# SQL query to perform the join and apply the logic
query = """
SELECT
    COALESCE(df.id, new_df.id) AS id,
    COALESCE(new_df.name, df.name) AS name,
    COALESCE(new_df.value, df.value) AS value,
    CASE
        WHEN new_df.id IS NOT NULL AND df.id IS NULL THEN 'new'
        WHEN new_df.id IS NOT NULL AND df.id IS NOT NULL THEN 'updated'
        ELSE 'old'
    END AS status
FROM
    df
FULL OUTER JOIN
    new_df
ON
    df.id = new_df.id
"""

# Execute the query
merged_df = spark.sql(query)

# Show the result
merged_df.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e839bbd6-3d1f-4c4e-8c1d-d20948a57f3b",
# META       "default_lakehouse_name": "LearningLab",
# META       "default_lakehouse_workspace_id": "b5ef7d0f-3e85-42bd-adc9-1df333730295",
# META       "known_lakehouses": [
# META         {
# META           "id": "e839bbd6-3d1f-4c4e-8c1d-d20948a57f3b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Import types
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GEnerate data 
# Generate comma delimited data
stringCSVRDD = sc.parallelize([
(123, 'Katie', 19, 'brown'),
(234, 'Michael', 22, 'green'), 
(345, 'Simone', 23, 'blue')
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Specify schema
schema = StructType([
StructField("id", LongType(), True),    
StructField("name", StringType(), True),
StructField("age", LongType(), True),
StructField("eyeColor", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Apply the schema to the RDD and Create DataFrame
swimmers = spark.createDataFrame(stringCSVRDD, schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creates a temporary view using the DataFrame
swimmers.createOrReplaceTempView("swimmers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

swimmers.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

swimmers.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

swimmers.select("id","age").filter("age=22").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

swimmers.select(swimmers.id, swimmers.age).filter(swimmers.age == 22).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("select count(1) from swimmers").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("select id, age from swimmers where age = 22").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

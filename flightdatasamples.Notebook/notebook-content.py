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
# META         },
# META         {
# META           "id": "afba8d44-ccd2-4e03-b969-396041be2dda"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Flightperf = spark.read.format("csv").option("header","true").load("abfss://MasteringSpark@onelake.dfs.fabric.microsoft.com/LearningLab.Lakehouse/Files/departuredelays.csv")
# df now is a Spark DataFrame containing CSV data from "abfss://MasteringSpark@onelake.dfs.fabric.microsoft.com/LearningLab.Lakehouse/Files/departuredelays.csv".
display(Flightperf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Flightperf.createOrReplaceTempView("FlighPerformancevw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Flightperf.cache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

airports = spark.read.csv("abfss://MasteringSpark@onelake.dfs.fabric.microsoft.com/LearningLab.Lakehouse/Files/airport-codes-na.txt",header='True',inferSchema='true',sep='\t')
# df now is a Spark DataFrame containing text data from "abfss://MasteringSpark@onelake.dfs.fabric.microsoft.com/LearningLab.Lakehouse/Files/airport-codes-na.txt".
display(airports)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

airports.createOrReplaceTempView("airportsvw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Query Sum of Flight Delays by City and Origin Code 
# (for Washington State)
spark.sql("""
select a.City, 
f.origin, 
sum(f.delay) as Delays 
from FlighPerformancevw f 
join airportsvw a 
on a.IATA = f.origin
where a.State = 'WA'
group by a.City, f.origin
order by sum(f.delay) desc"""
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Query Sum of Flight Delays by City and Origin Code (for Washington State)
# MAGIC select a.City, f.origin, sum(f.delay) as Delays
# MAGIC   from FlighPerformancevw f
# MAGIC     join airportsvw a
# MAGIC       on a.IATA = f.origin
# MAGIC  where a.State = 'WA'
# MAGIC  group by a.City, f.origin
# MAGIC  order by sum(f.delay) desc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Query Sum of Flight Delays by City and Origin Code (for Washington State)
# MAGIC select a.state, sum(f.delay) as Delays
# MAGIC   from FlighPerformancevw f
# MAGIC     join airportsvw a
# MAGIC       on a.IATA = f.origin
# MAGIC  where a.Country = 'USA'
# MAGIC  group by a.State
# MAGIC  order by sum(f.delay) desc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

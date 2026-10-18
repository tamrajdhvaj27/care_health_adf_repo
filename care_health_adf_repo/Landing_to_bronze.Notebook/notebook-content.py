# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d469d422-3968-4c3f-b857-9c604974cca0",
# META       "default_lakehouse_name": "LH_HealthCareProject",
# META       "default_lakehouse_workspace_id": "f551351a-039d-464d-905f-36c603442223",
# META       "known_lakehouses": [
# META         {
# META           "id": "d469d422-3968-4c3f-b857-9c604974cca0"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

source_system = 'neon_postgres'
table_name = 'hospitals'
landing_timestamp='2026-10-15_12:06:37'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfs = spark.read.format("parquet").load(f'abfss://Health_Care_Project@onelake.dfs.fabric.microsoft.com/LH_HealthCareProject.Lakehouse/Files/Landing/{source_system}/{table_name}/data_{landing_timestamp}')
# display(landing_dfs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp,lit

landing_df=  dfs.withColumn("landing_timestamp",lit(landing_timestamp))\
                .withColumn("insert_timestamp",current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

landing_df.write \
    .mode("append") \
    .saveAsTable(f"bronze.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

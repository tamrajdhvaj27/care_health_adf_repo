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

landing_timestamp = '2026-10-18_12:07:51'
table_name='patients'
merge_keys = 'patient_id'
load_types  = 'MERGE'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df = spark.read.table(f"bronze.{table_name}").filter(f"landing_timestamp='{landing_timestamp}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql.functions import current_timestamp
# updated_df = bronze_df.withColumn("insert_timestamp",current_timestamp()).withColumn("updated_timestamp",current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# updated_df.write.mode('overwrite').saveAsTable(f'silver.{table_name}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp
target_table = f'silver.{table_name}'
normalized_load_type = (load_types or "FULL").strip().upper()
merge_key_list = [key.strip() for key in merge_keys.split(",") if key.strip()]

if normalized_load_type == "MERGE":
    if not merge_key_list:
        raise ValueError("merge_keys must be provided when load_type is 'MERGE'.")

    missing_merge_keys = [key for key in merge_key_list if key not in bronze_df.columns]
    if missing_merge_keys:
        raise ValueError(f"merge_keys not found in source data:{missing_merge_keys}")
updated_df = (
     bronze_df.withColumn("insert_timestamp",current_timestamp()).withColumn("updated_timestamp",current_timestamp())
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if normalized_load_type == "FULL":
    updated_df.write.mode("overwrite").saveAsTable(target_table)

elif normalized_load_type == "APPEND":
    updated_df.write.mode("append").saveAsTable(target_table)

elif normalized_load_type == "MERGE":
    if not spark.catalog.tableExists(target_table):
        updated_df.write.mode("overwrite").saveAsTable(target_table)
    else:
        source_view = "silver_merge_source"
        updated_df.createOrReplaceTempView(source_view)

        merge_condition = " AND ".join(
            [f"t.{key}= s.{key}" for key in merge_key_list]
        )
        update_assigments = ", ".join(
            [
                f"t.{column} = s.{column}"
                for column in updated_df.columns
                if column != "insert_timestamp"
            ]
        )
        insert_columns = ", ".join([f"{column}" for column in updated_df.columns])
        insert_values = ", ".join([f"s.{column}" for column in updated_df.columns])

        spark.sql(f"""
            MERGE INTO {target_table} AS t
            USING {source_view} as s
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_assigments}
            WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES({insert_values})
        """)
else:
    raise ValueError("load_type must be one of: FULL, APPEND, MERGE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

record_count = updated_df.count()

mssparkutils.notebook.exit(record_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

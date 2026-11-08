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

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE gold.patient_360 AS
# MAGIC 
# MAGIC WITH lab_results_agg AS (
# MAGIC     SELECT
# MAGIC     patient_id,
# MAGIC     cast(count(*) AS int) as total_tests,
# MAGIC     cast(sum(case when result_status = 'Abnormal' then 1 else 0 end) as int) as abnormal_test_count,
# MAGIC     cast(sum(case when result_status = 'Critical' then 1 else 0 end) as int ) as critical_test_count,
# MAGIC     max(test_date) as last_test_date
# MAGIC     FROM silver.lab_results
# MAGIC     group by patient_id
# MAGIC     ORDER BY patient_id
# MAGIC )
# MAGIC SELECT 
# MAGIC p.patient_id,
# MAGIC p.first_name,
# MAGIC p.last_name,
# MAGIC cast(p.date_of_birth as date) as date_of_birth,
# MAGIC cast(floor(datediff(current_date(),cast(p.date_of_birth as date))/365.25)as int) as Age,
# MAGIC p.gender,
# MAGIC p.city,
# MAGIC p.state,
# MAGIC p.registered_hospital_id as hosptial_id,
# MAGIC h.hospital_name,
# MAGIC p.insurance_provider_id,
# MAGIC ip.provider_name as insurance_provider_name,
# MAGIC coalesce(lra.total_tests,0) as total_tests,
# MAGIC coalesce(lra.abnormal_test_count,0) as abnormal_test_count,
# MAGIC coalesce(lra.critical_test_count,0) as critical_test_count,
# MAGIC lra.last_test_date
# MAGIC FROM silver.patients as p
# MAGIC LEFT JOIN silver.hospitals as h 
# MAGIC     ON p.registered_hospital_id=h.hospital_id
# MAGIC LEFT JOIN silver.insurance_providers as ip
# MAGIC     ON p.insurance_provider_id = ip.insurance_provider_id
# MAGIC LEFT JOIN lab_results_agg as lra
# MAGIC     ON p.patient_id=lra.patient_id


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

updated_df = spark.table("gold.patient_360")
record_count = updated_df.count()
mssparkutils.notebook.exit(str(record_count))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

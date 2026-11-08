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
# MAGIC -- Create hosptial daily summary
# MAGIC 
# MAGIC -- CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE gold.hospital_daily_summary as 
# MAGIC with daily_metrics as (
# MAGIC     select 
# MAGIC         hospital_id,
# MAGIC         test_date as summary_date,
# MAGIC         cast(count(*) as int) as total_tests,
# MAGIC         cast(count(distinct patient_id) as int) as unique_patients_tested,
# MAGIC         cast(count(distinct doctor_id) as int) as active_doctor_count,
# MAGIC         cast(sum(case when result_status = 'Abnormal' then 1 else 0 end ) as int ) as abnormal_test_count,
# MAGIC         cast(sum(case when result_status = 'Critical' then 1 else 0 end ) as int ) as critical_test_count
# MAGIC     from silver.lab_results
# MAGIC     group by hospital_id,test_date
# MAGIC ),
# MAGIC enriched AS(
# MAGIC     select 
# MAGIC         dm.hospital_id,
# MAGIC         h.hospital_name,
# MAGIC         dm.summary_date,
# MAGIC         c.day_name,
# MAGIC         c.is_weekend,
# MAGIC         dm.total_tests,
# MAGIC         dm.unique_patients_tested,
# MAGIC         dm.active_doctor_count,
# MAGIC         dm.abnormal_test_count,
# MAGIC         dm.critical_test_count,
# MAGIC         cast(case when dm.total_tests = 0 then null else round(dm.abnormal_test_count * 100.0 / dm.total_tests,2) end as decimal(5,2)) as abnormal_rate_pct,
# MAGIC         cast(case when dm.total_tests = 0 then null else round(dm.critical_test_count * 100.0 / dm.total_tests, 2) end as decimal(5,2)) as critical_rate_pct
# MAGIC     from daily_metrics as dm
# MAGIC     left join silver.hospitals as h
# MAGIC         on dm.hospital_id = h.hospital_id
# MAGIC     left join silver.calendar as c
# MAGIC         on cast(date_format(dm.summary_date,'yyyyMMdd') as int ) = c.date_key
# MAGIC )
# MAGIC select 
# MAGIC     hospital_id,
# MAGIC     hospital_name,
# MAGIC     summary_date,
# MAGIC     day_name,
# MAGIC     is_weekend,
# MAGIC     total_tests,
# MAGIC     unique_patients_tested,
# MAGIC     active_doctor_count,
# MAGIC     critical_test_count,
# MAGIC     abnormal_rate_pct,
# MAGIC     critical_rate_pct,
# MAGIC     cast(lag(total_tests) over(partition by hospital_id order by summary_date) as int) as prev_day_total_tests,
# MAGIC     cast(
# MAGIC         case 
# MAGIC             when 
# MAGIC                 lag(total_tests) over(partition by hospital_id order by summary_date ) is null 
# MAGIC                 or lag(total_tests) over(partition by hospital_id order by summary_date ) = 0 then Null 
# MAGIC             else  
# MAGIC                 round(
# MAGIC                     (total_tests - lag(total_tests) over(partition by hospital_id order by summary_date)) *100.0
# MAGIC                     / lag(total_tests) over(partition by hospital_id order by summary_date), 2
# MAGIC             )
# MAGIC         end as decimal(6,2)
# MAGIC     ) as test_volume_change_pct
# MAGIC from enriched

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

updated_df = spark.table('gold.hospital_daily_summary')
record_count = updated_df.count()
mssparkutils.notebook.exit(str(record_count))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

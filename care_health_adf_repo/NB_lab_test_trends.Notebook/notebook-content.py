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
# MAGIC CREATE OR REPLACE TABLE gold.lab_test_trends AS
# MAGIC WITH monthly_metrics AS (
# MAGIC     SELECT
# MAGIC     test_name,
# MAGIC     year(test_date) as year,
# MAGIC     month(test_date) as month,
# MAGIC     date_trunc('month',test_date) as month_start_date,
# MAGIC     cast(count(*) as int) as total_tests,
# MAGIC     cast(sum(case when result_status = "Abnormal" then 1 else 0 end ) as int) as abnormal_test_count,
# MAGIC     cast(sum(case when result_status = "Critical" then 1 else 0 end) as int ) as critical_test_count
# MAGIC     FROM silver.lab_results
# MAGIC     GROUP by test_name,year(test_date), month(test_date),date_trunc('month',test_date)
# MAGIC )
# MAGIC ,enriched as (
# MAGIC     SELECT
# MAGIC     mm.test_name,
# MAGIC     mm.year,
# MAGIC     mm.month,
# MAGIC     coalesce(c.month_name,date_format(mm.month_start_date,'MMMM')) as month_name,
# MAGIC     concat('Q',QUARTER(mm.month_start_date)) as quater_name,
# MAGIC     mm.total_tests,
# MAGIC     mm.abnormal_test_count,
# MAGIC     mm.critical_test_count,
# MAGIC     cast(
# MAGIC         case when mm.total_tests = 0 then null else round(mm.abnormal_test_count*100.0 /mm.total_tests,2) end as DECIMAL(5,2)
# MAGIC         ) as abnormal_rate_pct,
# MAGIC     cast( 
# MAGIC         case when mm.total_tests = 0 then null else round(mm.critical_test_count*100.0/ mm.total_tests,2) end as DECIMAL(5,2)
# MAGIC         ) as critical_rate_pct
# MAGIC     From monthly_metrics as mm 
# MAGIC     LEFT JOIN silver.calendar as c 
# MAGIC         on cast(date_format(mm.month_start_date,'yyyyMMdd') as int ) = c.date_key
# MAGIC 
# MAGIC )
# MAGIC select 
# MAGIC     test_name,
# MAGIC     year,
# MAGIC     month,
# MAGIC     month_name,
# MAGIC     quater_name,
# MAGIC     total_tests,
# MAGIC     abnormal_test_count,
# MAGIC     critical_test_count,
# MAGIC     abnormal_rate_pct,
# MAGIC     critical_rate_pct,
# MAGIC     cast(lag(abnormal_rate_pct) over(partition by test_name order by year,month)as decimal(5,2))as prev_month_abnormal_rate_pct,
# MAGIC     cast( 
# MAGIC         case when Lag(abnormal_rate_pct) over(partition by test_name order by year, month) is null then null else
# MAGIC         round ( abnormal_rate_pct - Lag (abnormal_rate_pct) over(partition by test_name order by year, month),2) end
# MAGIC         as decimal(5,2)) as abnormal_rate_change_pct
# MAGIC from enriched;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

updated_df = spark.table('gold.lab_test_trends')
record_count = updated_df.count()
mssparkutils.notebook.exit(str(record_count))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
# MAGIC CREATE or REPLACE TABLE silver.calendar as
# MAGIC WITH dates as(
# MAGIC     SELECT explode(
# MAGIC         sequence(
# MAGIC             to_date('1940-01-01'),
# MAGIC             to_date('2030-12-31'),
# MAGIC             INTERVAL 1 days
# MAGIC         )
# MAGIC     ) as calendar_date
# MAGIC )
# MAGIC SELECT
# MAGIC   CAST(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC   calendar_date AS date,
# MAGIC   year(calendar_date) AS year,
# MAGIC   quarter(calendar_date) AS quarter,
# MAGIC   ((month(calendar_date) - 1) % 3) + 1 AS month_of_quarter,
# MAGIC   month(calendar_date) AS month,
# MAGIC   date_format(calendar_date, 'MMMM') AS month_name,
# MAGIC   date_format(calendar_date, 'MMM') AS month_name_short,
# MAGIC   concat(year(calendar_date), '-', lpad(month(calendar_date), 2, '0')) AS year_month,
# MAGIC   concat(year(calendar_date), lpad(month(calendar_date), 2, '0')) AS year_month_key,
# MAGIC   weekofyear(calendar_date) AS week_of_year,
# MAGIC   concat(year(calendar_date), '-W', lpad(weekofyear(calendar_date), 2, '0')) AS year_week,
# MAGIC   day(calendar_date) AS day_of_month,
# MAGIC   dayofyear(calendar_date) AS day_of_year,
# MAGIC   weekday(calendar_date) + 1 AS iso_day_of_week,
# MAGIC   dayofweek(calendar_date) AS us_day_of_week,
# MAGIC   date_format(calendar_date, 'EEEE') AS day_name,
# MAGIC   date_format(calendar_date, 'E') AS day_name_short,
# MAGIC   CASE WHEN dayofweek(calendar_date) IN (1, 7) THEN true ELSE false END AS is_weekend,
# MAGIC   CASE WHEN calendar_date = trunc(calendar_date, 'month') THEN true ELSE false END AS is_month_start,
# MAGIC   CASE WHEN calendar_date = last_day(calendar_date) THEN true ELSE false END AS is_month_end,
# MAGIC   CASE WHEN calendar_date = trunc(calendar_date, 'quarter') THEN true ELSE false END AS is_quarter_start,
# MAGIC   CASE WHEN calendar_date = date_sub(add_months(trunc(calendar_date, 'quarter'), 3), 1) THEN true ELSE false END AS is_quarter_end,
# MAGIC   CASE WHEN calendar_date = trunc(calendar_date, 'year') THEN true ELSE false END AS is_year_start,
# MAGIC   CASE WHEN calendar_date = date_sub(add_months(trunc(calendar_date, 'year'), 12), 1) THEN true ELSE false END AS is_year_end,
# MAGIC   trunc(calendar_date, 'week') AS week_start_date,
# MAGIC   date_add(trunc(calendar_date, 'week'), 6) AS week_end_date,
# MAGIC   trunc(calendar_date, 'month') AS month_start_date,
# MAGIC   last_day(calendar_date) AS month_end_date,
# MAGIC   trunc(calendar_date, 'quarter') AS quarter_start_date,
# MAGIC   date_sub(add_months(trunc(calendar_date, 'quarter'), 3), 1) AS quarter_end_date,
# MAGIC   trunc(calendar_date, 'year') AS year_start_date,
# MAGIC   date_sub(add_months(trunc(calendar_date, 'year'), 12), 1) AS year_end_date,
# MAGIC   CASE WHEN (year(calendar_date) % 400 = 0) OR (year(calendar_date) % 4 = 0 AND year(calendar_date) % 100 <> 0) THEN true ELSE false END AS is_leap_year
# MAGIC FROM dates;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   MIN(date) AS min_date,
# MAGIC   MAX(date) AS max_date,
# MAGIC   MIN(date_key) AS min_date_key,
# MAGIC   MAX(date_key) AS max_date_key,
# MAGIC   COUNT(DISTINCT year) AS distinct_years,
# MAGIC   COUNT(DISTINCT year_month_key) AS distinct_months
# MAGIC FROM silver.calendar;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


%%sql

SELECT *
FROM silver.calendar
ORDER BY date
LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

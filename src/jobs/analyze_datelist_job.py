from pyspark.sql import SparkSession

REF_DATE = "2023-03-31"  # change if needed

query = f"""
WITH starter AS (
  SELECT
    user_id,
    /* make sure each element is DATE even if the source is ARRAY<STRING> */
    transform(dates_active, x -> to_date(x)) AS dates_active_cast,
    DATE('{REF_DATE}') AS ref_date,
    sequence(date_sub(DATE('{REF_DATE}'), 31), DATE('{REF_DATE}')) AS valid_dates
  FROM users_cumulated
  WHERE cast(date as date) = DATE('{REF_DATE}')
),
exploded AS (
  SELECT
    user_id,
    d AS valid_date,
    array_contains(dates_active_cast, d) AS is_active,
    datediff(DATE('{REF_DATE}'), d) AS days_since
  FROM starter
  LATERAL VIEW explode(valid_dates) v AS d
),
bits AS (
  SELECT
    user_id,
    SUM(CASE WHEN is_active AND days_since BETWEEN 0 AND 31
             THEN shiftleft(1L, 31 - days_since) ELSE 0L END) AS datelist_int
  FROM exploded
  GROUP BY user_id
)
SELECT
  user_id,
  datelist_int,
  bit_count(datelist_int)                              AS monthly_active,
  bit_count( (datelist_int >> 25) & 127L )             AS weekly_active,
  CAST(bit_count( (datelist_int >> 25) & 127L ) > 0 AS BOOLEAN) AS weekly_active_flag,
  CAST(bit_count( (datelist_int >> 18) & 127L ) > 0 AS BOOLEAN) AS weekly_active_previous_week
FROM bits
"""

def do_analyze_datelist_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("users_cumulated")
    return spark.sql(query)

def main():
    spark = SparkSession.builder.master("local").appName("analyze_datelist").getOrCreate()
    output_df = do_analyze_datelist_transformation(spark, spark.table("bootcamp.users_cumulated"))
    output_df.write.mode("overwrite").saveAsTable("bootcamp.analyze_datelist_results")

from pyspark.sql import SparkSession

query = """
-- This query creates a slowly changing dimension (SCD) for view teams by selecting unique team records
-- and ensuring that only the first occurrence of each team_id is retained.
WITH teams_deduped AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id order by team_id) as row_num
    FROM teams
)
SELECT
       team_id AS identifier,
    'team' AS `type`,
    map(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', CAST(yearfounded as STRING)
        ) AS properties
FROM teams_deduped
WHERE row_num = 1
"""

def dff_team_vertex_transformation(spark, dataframe):
    """
    This function transforms the input dataframe to create a slowly changing dimension (SCD) for players.
    It uses a SQL query to select and transform the necessary fields.
    """
    # Register the input dataframe as a temporary view
    dataframe.createOrReplaceTempView("teams")
    
    # Execute the SQL query to get the transformed dataframe
    return spark.sql(query)
    

# main is where we actually run the spark job
def main():
    spark = SparkSession.builder \
        .master ("local") \
        .appName("player_scd") \
        .getOrCreate()
    Output_df = dff_team_vertex_transformation(spark, spark.table("players"))
    output_df.write.mode("overwrite").insertInto("player_scd")
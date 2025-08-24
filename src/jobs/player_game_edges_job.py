from pyspark.sql import SparkSession

query = """
WITH dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY player_id, game_id ORDER BY game_id) AS row_num
  FROM game_details
)
SELECT
  CAST(player_id AS STRING) AS subject_identifier,
  'player'                  AS subject_type,
  CAST(game_id  AS STRING)  AS object_identifier,
  'game'                    AS object_type,
  'plays_in'                AS edge_type,
  /* keep values nullable to match expected schema */
  map(
    'start_position', CAST(start_position AS STRING),
    'pts',            CAST(pts AS STRING),
    'ast',            CAST(ast AS STRING),
    'stl',            CAST(stl AS STRING),
    'team_id',        CAST(team_id AS STRING),
    'team_abbreviation', CAST(team_abbreviation AS STRING)
  ) AS properties
FROM dedup
WHERE row_num = 1
"""

def do_player_game_edges_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)

def main():
    spark = SparkSession.builder.master("local").appName("player_game_edges").getOrCreate()
    output_df = do_player_game_edges_transformation(spark, spark.table("bootcamp.game_details"))
    output_df.write.mode("overwrite").saveAsTable("bootcamp.player_game_edges")

from chispa.dataframe_comparer import *
from collections import namedtuple
from ..jobs.player_game_edges_job import do_player_game_edges_transformation

GameDetail = namedtuple(
    "GameDetail",
    "game_id team_id team_abbreviation player_id start_position pts ast stl"
)
Edge = namedtuple(
    "Edge",
    "subject_identifier subject_type object_identifier object_type edge_type properties"
)

def test_player_game_edges_basic(spark):
    # two rows for (player_id=100, game_id=1) -> should dedupe to one edge
    input_data = [
        GameDetail(1, 10, "ABC", 100, "C", 20, 5, 1),
        GameDetail(1, 10, "ABC", 100, "C", 20, 5, 1),
        GameDetail(2, 20, "XYZ", 200, "G", 11, 7, 3),
    ]
    input_df = spark.createDataFrame(input_data)

    actual_df = do_player_game_edges_transformation(spark, input_df).orderBy("subject_identifier","object_identifier")

    expected = [
        Edge(
            subject_identifier="100",
            subject_type="player",
            object_identifier="1",
            object_type="game",
            edge_type="plays_in",
            properties={
                "start_position":"C","pts":"20","ast":"5","stl":"1",
                "team_id":"10","team_abbreviation":"ABC"
            }
        ),
        Edge(
            subject_identifier="200",
            subject_type="player",
            object_identifier="2",
            object_type="game",
            edge_type="plays_in",
            properties={
                "start_position":"G","pts":"11","ast":"7","stl":"3",
                "team_id":"20","team_abbreviation":"XYZ"
            }
        ),
    ]
    expected_df = spark.createDataFrame(expected).orderBy("subject_identifier","object_identifier")

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

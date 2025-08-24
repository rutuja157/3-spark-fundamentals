from chispa.dataframe_comparer import *

from ..jobs.rc_team_vertex_job import dff_team_vertex_transformation
from collections import namedtuple
#create schema for the input data
TeamVertex = namedtuple("TeamVertex", "identifier type properties")
Team = namedtuple("Team", "team_id abbreviation nickname city arena yearfounded")

## every test function should start with test_ so pytest can discover it
## we need to crate input data for the test and then we can use the function we defined in the job to do the transformation
##but we need to comeup with the schema 1st,in this case we have tesm schema and team_vertext schema 
## of the input data so it matches the expected input schema of the function

#now will create fake input data for the test


def test_vertex_generation(spark):
    input_data = [
        Team(1, "GSW", "Warriors", "San Francisco", "Chase Center", 1900),
        Team(1, "GSW", "Bad Warriors", "San Francisco", "Chase Center", 1900),
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = dff_team_vertex_transformation(spark, input_dataframe) # this is actual output dataframe 
    # Define the expected output
    # we need to create the expected output data with the same schema as the actual output
    # so we can compare the actual output with the expected output
    expected_output = [
        TeamVertex(
            identifier=1,
            type='team',
            properties={
                'abbreviation': 'GSW',
                'nickname': 'Warriors',
                'city': 'San Francisco',
                'arena': 'Chase Center',
                'year_founded': '1900'
            }
        )
    ]

    #this is the transformation we expect to get from the function with given input_data
    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
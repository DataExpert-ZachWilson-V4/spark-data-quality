    from chispa.dataframe_comparer import *
    from collections import namedtuple
    from ..jobs.job_1 import job_1
    from ..jobs.job_2 import job_2
    from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, BooleanType, FloatType, IntegerType

    actor_films = namedtuple("actor_films", "actor actor_id film year votes rating film_id")
    actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")

    def test_job_one(spark_session):
        output_table_name: str = "actors"
        input_table_name: str = "actor_films"
        this_year: str = "1929"
    
        input_schema = StructType([
            StructField("actor", StringType()),
            StructField("actor_id", StringType()),
            StructField("film", StringType()),
            StructField("year", StringType()),
            StructField("votes", StringType()),
            StructField("rating", StringType()),
            StructField("film_id", StringType())      
        ])


        input_data = [
            actor_films("Marlene Dietrich","nm0000017","I Kiss Your Hand Madame","1929","123","6.1","tt0020011"),
            actor_films("Buster Keaton","nm0000036","Spite Marriage","1929","1913","7.0","tt0020442"),
            actor_films("Buster Keaton","nm0000036","The Hollywood Revue of 1929","1929","1856","5.9","tt0019993")
        ]

        input_df = spark_session.createDataFrame(input_data, input_schema)

        output_schema = StructType([
            StructField("actor", StringType()),
            StructField("actor_id", StringType()),
            StructField("films", ArrayType(StringType())),
            StructField("quality_class", StringType()),
            StructField("is_active", BooleanType()),
            StructField("current_year", StringType())    
        ])

        expected_schema = StructType([
            StructField("actor", StringType()),
            StructField("actor_id", StringType()),
            StructField("films", ArrayType(StringType())),
            StructField("quality_class", StringType()),
            StructField("is_active", BooleanType()),
            StructField("current_year", IntegerType())    
        ])


        expected_output = [
            actors(
            "Marlene Dietrich",
            "nm0000017",
            [["I Kiss Your Hand Madame","123","6.1","tt0020011","1929"]],
            "average",
            True,
            1929
            ),

            actors(
            "Buster Keaton",
            "nm0000036",
            [["Spite Marriage","1913","7.0","tt0020442","1929"],
            ["The Hollywood Revue of 1929","1856","5.9","tt0019993","1929"]],
            "average",
            True,
            1929
            )
            ,
            actors(
            "Charles Chaplin",
            "nm0000122",
            [["The Circus","30668","8.1","tt0018773","1928"],["Show People","3505","7.6","tt0019379","1928"]],
            "good",
            False,
            1929
            )
        ]


        last_year_output = [

            actors(
            "Charles Chaplin",
            "nm0000122",
            [["The Circus","30668","8.1","tt0018773","1928"],["Show People","3505","7.6","tt0019379","1928"]],
            "good",
            True,
            "1928"
            )
        ]

        output_df = spark_session.createDataFrame(last_year_output, output_schema)

        actual_df = job_1(spark_session,output_df,output_table_name,input_df, input_table_name,this_year)
        expected_df = spark_session.createDataFrame(expected_output,expected_schema )

        assert_df_equality(actual_df,expected_df,ignore_nullable= True)


    GameDetails = namedtuple("GameDetails","game_id team_id team_abbreviation team_city player_id player_name start_position comment min fgm fga fg3m fg3a ftm fta oreb dreb reb ast stl blk to pf pts plus_minus")
    OutputDetails = namedtuple("GameDetails","game_id team_id team_abbreviation team_city player_id player_name start_position dim_did_not_dress dim_not_with_team min fgm fga fg3m fg3a ftm fta oreb dreb reb ast stl blk to pf pts plus_minus")
    
    def test_job_two(spark_session):
        input_table_name: str = "nba_game_details"

        input_data = [
            GameDetails("10300001","1610612742","DAL","Dallas",	696	,"Travis Best"	,None,	None,	"5",	4,	7,	0,	0,	2,	4,	0,	1,	1,	5,	0,	0,	3,	6,	10,	None),
            GameDetails("10300001","1610612742","DAL","Dallas",	696	,"Travis Best"	,None,	None,	"5",	4,	7,	0,	0,	2,	4,	0,	1,	1,	5,	0,	0,	3,	6,	10,	None),
            GameDetails("11300047","1610612755","PHI","Philadelphia",201150,"Spencer Hawes",None,	None,	"17",	1,	7,	0,	2,	4,	4,	1,	4,	5,	2,	0,	0,	0,	3,	6,	None)
        ]
    
        input_df = spark_session.createDataFrame(input_data)
 
        expected_data = [
            OutputDetails("10300001","1610612742","DAL","Dallas",	696	,"Travis Best"	,None,	None,	"5",	4,	7,	0,	0,	2,	4,	0,	1,	1,	5,	0,	0,	3,	6,	10,	None),
            OutputDetails("11300047","1610612755","PHI","Philadelphia",201150,"Spencer Hawes",None,	None,	"17",	1,	7,	0,	2,	4,	4,	1,	4,	5,	2,	0,	0,	0,	3,	6,	None)
        ]
    
        expected_df = spark_session.createDataFrame(expected_data)
    
        result_df = job_2(spark_session,input_table_name, input_df)
    
        assert_df_equality(result_df, expected_df)
        

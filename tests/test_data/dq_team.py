%python
# test_data/dq_team.py

import pytest
from pyspark.sql import SparkSession
from src.dq_suite.common import get_full_table_name

def test_team_table_schema():
    # Create a SparkSession
    spark = SparkSession.builder.appName("test_team_table").getOrCreate()

    # Define the expected schema
    expected_schema = [
        ("teamId", "string"),
        ("teamName", "string"),
        ("teamDescription", "string")
    ]

    # Get the team table schema
    team_table_name = get_full_table_name(catalog_name="dpd1_dev", table_name="data_quality.team")
    team_table = spark.table(team_table_name)
    team_table_schema = team_table.schema

    # Assert that the schema matches the expected schema
    assert len(team_table_schema) == len(expected_schema)
    for i, (field_name, field_type) in enumerate(expected_schema):
        assert team_table_schema[i].name == field_name
        assert team_table_schema[i].dataType.typeName() == field_type

def test_team_table_data():
    # Create a SparkSession
    spark = SparkSession.builder.appName("test_team_table").getOrCreate()

    # Define some sample data
    sample_data = [
        ("team1", "Team 1", "This is team 1"),
        ("team2", "Team 2", "This is team 2")
    ]

    # Create a DataFrame with the sample data
    team_df = spark.createDataFrame(sample_data, ["teamId", "teamName", "teamDescription"])

    # Get the team table
    team_table_name = get_full_table_name(catalog_name="dpd1_dev", table_name="data_quality.team")
    team_table = spark.table(team_table_name)

    # Assert that the data matches the sample data
    assert team_df.count() == team_table.count()
    assert team_df.collect() == team_table.collect()

def test_team_table_unique_team_id():
    # Create a SparkSession
    spark = SparkSession.builder.appName("test_team_table").getOrCreate()

    # Get the team table
    team_table_name = get_full_table_name(catalog_name="dpd1_dev", table_name="data_quality.team")
    team_table = spark.table(team_table_name)

    # Assert that the teamId column is unique
    team_ids = team_table.select("teamId").distinct().collect()
    assert len(team_ids) == team_table.count()

def test_team_table_team_name_not_null():
    # Create a SparkSession
    spark = SparkSession.builder.appName("test_team_table").getOrCreate()

    # Get the team table
    team_table_name = get_full_table_name(catalog_name="dpd1_dev", table_name="data_quality.team")
    team_table = spark.table(team_table_name)

    # Assert that the teamName column is not null
    null_team_names = team_table.filter(team_table.teamName.isNull()).count()
    assert null_team_names == 0
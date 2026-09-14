import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import Row

from update_raw import (
    parse_page_numbers,
    construct_file_paths,
    create_full_snapshot_from_df,
    DuplicateOrNullIdentifierError,
)

def test_parse_page_numbers_valid():
    result = parse_page_numbers('["1", 2]')
    assert result == [1, 2]

def test_parse_page_numbers_invalid_json():
    with pytest.raises(ValueError):
        parse_page_numbers('not a json')

def test_parse_page_numbers_not_list():
    with pytest.raises(ValueError):
        parse_page_numbers('"1"')

def test_parse_page_numbers_non_int_elements():
    with pytest.raises(ValueError):
        parse_page_numbers('[1, "a"]')

@pytest.mark.parametrize("catalog,schema,run_id,pages,expected", [
    (
        "cat",
        "sch",
        "run42",
        [1, 3],
        [
            "/Volumes/cat/sch/landing/nwo_projects/run42_page1.json",
            "/Volumes/cat/sch/landing/nwo_projects/run42_page3.json",
        ],
    ),
    (
        "prod_catalog",
        "raw_schema",
        "job_001",
        [1],
        ["/Volumes/prod_catalog/raw_schema/landing/nwo_projects/job_001_page1.json"],
    ),
])
def test_construct_file_paths(catalog, schema, run_id, pages, expected):
    assert construct_file_paths(catalog, schema, run_id, pages) == expected

@pytest.fixture
def dataframe_raw(spark):
    return spark.createDataFrame([
    Row(
        meta='{"page": 1}',
        projects=[
            '{"project_id": "001", "title": "first instance"}', 
            '{"project_id": "002", "title": "some title"}'
        ]
    ),
    Row(
        meta='{"page": 2}',
        projects=[
            '{"project_id": "003", "title": "another title"}',
            '{"project_id": "004", "title": "the title"}'
        ]
    ),
    Row(
        meta='{"page": 3}',
        projects=[
            '{"project_id": "005", "title": "a title"}',
            '{"project_id": "001", "title": "second instance"}'
        ]
    )
])

def test_create_full_snapshot_from_df_raises_on_duplicate_id(dataframe_raw):
    with pytest.raises(DuplicateOrNullIdentifierError):
        create_full_snapshot_from_df(dataframe_raw)

@pytest.fixture
def dataframe_raw_with_null_id(spark):
    return spark.createDataFrame([
        Row(
            meta='{"page": 1}',
            projects=[
                '{"title": "missing project_id"}',
                '{"project_id": "002", "title": "some title"}'
            ]
        )
    ])

def test_create_full_snapshot_from_df_raises_on_null_id(dataframe_raw_with_null_id):
    with pytest.raises(DuplicateOrNullIdentifierError):
        create_full_snapshot_from_df(dataframe_raw_with_null_id)
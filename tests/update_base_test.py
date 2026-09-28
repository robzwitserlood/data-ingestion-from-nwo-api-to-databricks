import datetime

import pytest
from unittest.mock import MagicMock
from pyspark.sql import Row
from pyspark.testing import assertDataFrameEqual

from update_base import (
    COLUMN_SPECS,
    parse_project_columns,
    cleanse_missing_values,
    apply_types,
    set_freshness_timestamp,
)


@pytest.fixture
def raw_projects_df(spark):
    return spark.createDataFrame([
        Row(project_id="001", project_key="001|111|1|10", funding_scheme_id="111", leader_member_id="1", leader_organisation_id="10",
            project='{"title": "Study A", "department": "Physics", "sub_department": "Quantum", "reporting_year": "2020", "start_date": "2020-01-01", "end_date": "2021-01-01", "award_amount": "10000", "summary_nl": "Samenvatting", "summary_en": "Summary"}'),
        Row(project_id="002", project_key="002|222|2|20", funding_scheme_id="222", leader_member_id="2", leader_organisation_id="20",
            project='{"title": "Study B", "department": "n/a", "sub_department": "-", "reporting_year": "2021", "start_date": "2021-01-01", "end_date": "2022-01-01", "award_amount": "20000", "summary_nl": "", "summary_en": "   "}'),
    ])


IDENTITY_SCHEMA = "project_id string, project_key string, funding_scheme_id string, leader_member_id string, leader_organisation_id string"
IDENTITY_001 = ("001", "001|111|1|10", "111", "1", "10")
IDENTITY_002 = ("002", "002|222|2|20", "222", "2", "20")


def test_parse_project_columns(spark, raw_projects_df):
    expected = spark.createDataFrame(
        [
            IDENTITY_001 + ("Study A", "Physics", "Quantum", "2020", "2020-01-01", "2021-01-01", "10000", "Samenvatting", "Summary"),
            IDENTITY_002 + ("Study B", "n/a", "-", "2021", "2021-01-01", "2022-01-01", "20000", "", "   "),
        ],
        IDENTITY_SCHEMA + ", title string, department string, sub_department string, reporting_year string, "
        "start_date string, end_date string, award_amount string, summary_nl string, summary_en string",
    )
    assertDataFrameEqual(parse_project_columns(raw_projects_df, COLUMN_SPECS), expected)


@pytest.mark.parametrize("sentinel", ["", "  ", "n/a", "NA", "-", "—–", "NULL", "none"])
def test_cleanse_missing_values_nulls_sentinel(spark, sentinel):
    df = spark.createDataFrame([Row(project_id="001", title=sentinel)])
    row = cleanse_missing_values(df).collect()[0]
    assert row["title"] is None


def test_cleanse_missing_values_keeps_real_values(spark):
    df = spark.createDataFrame([Row(project_id="001", title="Real value")])
    row = cleanse_missing_values(df).collect()[0]
    assert row["project_id"] == "001"
    assert row["title"] == "Real value"


def test_parse_cleanse_and_apply_types(spark, raw_projects_df):
    parsed = parse_project_columns(raw_projects_df, COLUMN_SPECS)
    typed = apply_types(cleanse_missing_values(parsed), COLUMN_SPECS)
    expected = spark.createDataFrame(
        [
            IDENTITY_001 + ("Study A", "Physics", "Quantum", 2020, datetime.date(2020, 1, 1), datetime.date(2021, 1, 1), 10000, "Samenvatting", "Summary"),
            IDENTITY_002 + ("Study B", None, None, 2021, datetime.date(2021, 1, 1), datetime.date(2022, 1, 1), 20000, None, None),
        ],
        IDENTITY_SCHEMA + ", title string, department string, sub_department string, reporting_year int, "
        "start_date date, end_date date, award_amount int, summary_nl string, summary_en string",
    )
    assertDataFrameEqual(typed, expected)


def test_set_freshness_timestamp_calls_alter_table_with_correct_table_name():
    mock_spark = MagicMock()
    set_freshness_timestamp(mock_spark, "my_catalog.base.nwo_projects")
    mock_spark.sql.assert_called_once()
    call_args = mock_spark.sql.call_args[0][0]
    assert "ALTER TABLE my_catalog.base.nwo_projects" in call_args
    assert "pipeline.last_successful_refresh_utc" in call_args

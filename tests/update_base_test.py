import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import Row

from update_base import (
    COLUMN_SPECS,
    parse_project_columns,
    cleanse_missing_values,
    apply_types,
    set_freshness_timestamp,
)
from update_raw import DuplicateOrNullIdentifierError


@pytest.fixture
def raw_projects_df(spark):
    return spark.createDataFrame([
        Row(project_id="001", project_key="001|111|1|10", funding_scheme_id="111", leader_member_id="1", leader_organisation_id="10",
            project='{"title": "Study A", "department": "Physics", "sub_department": "Quantum", "reporting_year": "2020", "start_date": "2020-01-01", "end_date": "2021-01-01", "award_amount": "10000", "summary_nl": "Samenvatting", "summary_en": "Summary"}'),
        Row(project_id="002", project_key="002|222|2|20", funding_scheme_id="222", leader_member_id="2", leader_organisation_id="20",
            project='{"title": "Study B", "department": "n/a", "sub_department": "-", "reporting_year": "2021", "start_date": "2021-01-01", "end_date": "2022-01-01", "award_amount": "20000", "summary_nl": "", "summary_en": "   "}'),
    ])


def test_parse_project_columns(raw_projects_df):
    result = parse_project_columns(raw_projects_df, COLUMN_SPECS)
    expected_columns = {"project_id", "project_key", "funding_scheme_id", "leader_member_id", "leader_organisation_id"} | {alias for _, alias, _ in COLUMN_SPECS}
    assert set(result.columns) == expected_columns
    row = result.filter(result.project_id == "001").collect()[0]
    assert row["title"] == "Study A"
    assert row["department"] == "Physics"
    assert row["project_key"] == "001|111|1|10"


def test_cleanse_missing_values_normalizes_sentinels(spark):
    df = spark.createDataFrame([
        Row(project_id="001", title="", department="n/a", sub_department="-", summary_nl="NULL", summary_en="Real value"),
        Row(project_id="002", title="  ", department="NA", sub_department="—–", summary_nl="none", summary_en="Another value"),
    ])
    result = cleanse_missing_values(df)
    row1 = result.filter(result.project_id == "001").collect()[0]
    row2 = result.filter(result.project_id == "002").collect()[0]

    assert row1["title"] is None
    assert row1["department"] is None
    assert row1["sub_department"] is None
    assert row1["summary_nl"] is None
    assert row1["summary_en"] == "Real value"

    assert row2["title"] is None
    assert row2["department"] is None
    assert row2["sub_department"] is None
    assert row2["summary_nl"] is None
    assert row2["summary_en"] == "Another value"


def test_apply_types(raw_projects_df):
    parsed = parse_project_columns(raw_projects_df, COLUMN_SPECS)
    typed = apply_types(parsed, COLUMN_SPECS)
    row = typed.filter(typed.project_id == "001").collect()[0]
    assert isinstance(row["reporting_year"], int)
    assert row["reporting_year"] == 2020
    assert isinstance(row["award_amount"], int)
    assert row["award_amount"] == 10000


def test_apply_types_and_validation_raises_on_duplicate_id(spark):
    from update_raw import assert_unique_not_null_ids
    df = spark.createDataFrame([
        Row(project_id="001", title="A"),
        Row(project_id="001", title="B"),
    ])
    with pytest.raises(DuplicateOrNullIdentifierError):
        assert_unique_not_null_ids(df, 'project_id')


def test_apply_types_and_validation_raises_on_null_id(spark):
    from update_raw import assert_unique_not_null_ids
    df = spark.createDataFrame([
        Row(project_id=None, title="A"),
        Row(project_id="002", title="B"),
    ])
    with pytest.raises(DuplicateOrNullIdentifierError):
        assert_unique_not_null_ids(df, 'project_id')


def test_set_freshness_timestamp_calls_alter_table_with_correct_table_name():
    mock_spark = MagicMock()
    set_freshness_timestamp(mock_spark, "my_catalog.base.nwo_projects")
    mock_spark.sql.assert_called_once()
    call_args = mock_spark.sql.call_args[0][0]
    assert "ALTER TABLE my_catalog.base.nwo_projects" in call_args
    assert "pipeline.last_successful_refresh_utc" in call_args

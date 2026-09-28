import argparse
import json
from typing import List

from pyspark.sql.functions import (
    explode, col, get_json_object, md5, concat_ws, coalesce, lit, size, when,
    from_json, array_sort, filter as array_filter
)
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from delta.tables import DeltaTable
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession, DataFrame

LEADER_FIELDS_SCHEMA = 'array<struct<role:string,member_id:long,organisation_id:long>>'

class DuplicateOrNullIdentifierError(ValueError):
    """Raised when a DataFrame's identifier column contains null or duplicate values."""

def get_spark() -> SparkSession:
  spark = DatabricksSession.builder.getOrCreate()
  return spark

def parse_page_numbers(page_numbers: str) -> List[int]:
    """Parses the page numbers argument which can be a JSON array."""
    try:
        result = json.loads(page_numbers)
    except json.JSONDecodeError:
        raise ValueError("page_numbers must be a JSON array")

    if not isinstance(result, list):
        raise ValueError("page_numbers must be a JSON array")

    try:
        return [int(i) for i in result]
    except (TypeError, ValueError):
        raise ValueError("page_numbers must be a JSON array of integers")

def construct_file_paths(catalog: str, schema: str, run_id: str, page_numbers: List[int]) -> List[str]:
    """Constructs file paths for the given catalog, schema, run_id, and page numbers."""
    return [f'/Volumes/{catalog}/{schema}/landing/nwo_projects/{run_id}_page{i}.json' for i in page_numbers]


def read_json_projects(spark: SparkSession, paths: List[str]) -> DataFrame:
    """Reads the JSON files at the given paths into a DataFrame using the expected schema."""
    schema = StructType([
        StructField('projects', ArrayType(StringType()), True),
        StructField('meta', StringType(), True)
    ])
    return spark.read.format('json').schema(schema).load(paths)

def assert_unique_not_null_ids(df: DataFrame, id_col: str = 'project_id', check_duplicates: bool = True) -> DataFrame:
    """Raises DuplicateOrNullIdentifierError if id_col has any null values, or
    (when check_duplicates is True) any duplicate values.

    Names the count of null values and the count of duplicated values found.
    Returns df unchanged if the check passes.
    """
    null_count = df.filter(col(id_col).isNull()).count()
    duplicate_count = 0
    if check_duplicates:
        duplicate_count = (
            df.groupBy(id_col)
              .count()
              .filter((col('count') > 1) & col(id_col).isNotNull())
              .count()
        )
    if null_count > 0 or duplicate_count > 0:
        expectation = "all values to be unique and non-null" if check_duplicates else "no null values"
        raise DuplicateOrNullIdentifierError(
            f"Column '{id_col}' has {null_count} null value(s) and {duplicate_count} "
            f"duplicated value(s); expected {expectation}"
        )
    return df

def add_identity_key_fields(df: DataFrame) -> DataFrame:
    """Adds funding_scheme_id, leader_member_id, leader_organisation_id, and project_key.

    NWOpen's project_id is not a true unique identifier: distinct, unrelated projects
    have been observed sharing the same project_id (see constitution Principle IV).
    project_key composes project_id with funding_scheme_id and the primary project
    leader's identity (the project_members entry with role "Project leader", lowest
    member_id/organisation_id first when a project lists more than one) to
    disambiguate those real collisions.
    """
    members = from_json(get_json_object(col('project'), '$.project_members'), LEADER_FIELDS_SCHEMA)
    leaders = array_sort(array_filter(members, lambda m: m['role'] == lit('Project leader')))
    return (
        df
        .withColumn('_leaders', leaders)
        .withColumn('_leader_count', size(col('_leaders')))
        .withColumn('funding_scheme_id', get_json_object(col('project'), '$.funding_scheme_id'))
        .withColumn(
            'leader_member_id',
            when(col('_leader_count') > 0, col('_leaders')[0]['member_id']).otherwise(None).cast('string')
        )
        .withColumn(
            'leader_organisation_id',
            when(col('_leader_count') > 0, col('_leaders')[0]['organisation_id']).otherwise(None).cast('string')
        )
        .withColumn(
            'project_key',
            concat_ws(
                '|',
                coalesce(col('project_id'), lit('')),
                coalesce(col('funding_scheme_id'), lit('')),
                coalesce(col('leader_member_id'), lit('')),
                coalesce(col('leader_organisation_id'), lit(''))
            )
        )
        .drop('_leaders', '_leader_count')
    )

def create_full_snapshot_from_df(df: DataFrame) -> DataFrame:
    """Transforms the raw projects DataFrame into the full snapshot DataFrame."""
    snapshot = (
        df.select(explode(col('projects')).alias('project'))
          .select(
              get_json_object(col('project'), '$.project_id').alias('project_id'),
              col('project'),
              md5(col('project')).alias('row_hash')
          )
    )
    keyed = add_identity_key_fields(snapshot)
    assert_unique_not_null_ids(keyed, 'project_id', check_duplicates=False)
    return assert_unique_not_null_ids(keyed, 'project_key').alias('full_snapshot')

def merge_into_delta_table(spark: SparkSession, full_snapshot: DataFrame, catalog: str, schema: str) -> None:
    """Merges the full snapshot DataFrame into the Delta table."""

    spark.catalog.setCurrentCatalog(catalogName=catalog)
    spark.catalog.setCurrentDatabase(dbName=schema)

    if spark.catalog.tableExists(tableName='nwo_projects'):
        target = DeltaTable.forName(spark, 'nwo_projects').alias('target')
        target \
            .merge(full_snapshot, 'target.project_key = full_snapshot.project_key') \
            .whenMatchedUpdate(
                condition='target.row_hash <> full_snapshot.row_hash',
                set={
                    'project_id': 'full_snapshot.project_id',
                    'project': 'full_snapshot.project',
                    'row_hash': 'full_snapshot.row_hash',
                    'project_key': 'full_snapshot.project_key',
                    'funding_scheme_id': 'full_snapshot.funding_scheme_id',
                    'leader_member_id': 'full_snapshot.leader_member_id',
                    'leader_organisation_id': 'full_snapshot.leader_organisation_id'
                    }
                ) \
            .whenNotMatchedInsertAll() \
            .whenNotMatchedBySourceDelete() \
            .execute()
    else:
        full_snapshot.write.saveAsTable('nwo_projects')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and store NWO project data.")
    parser.add_argument("--page_numbers", required=True, type=str, help="Page numbers of the data to fetch")
    parser.add_argument("--run_id", required=True, type=str, help="Job run identifier used as prefix for the JSON file")
    parser.add_argument("--catalog", required=True, type=str, help="Catalog name for the directory structure")
    parser.add_argument("--schema", required=True, type=str, help="Schema name for the directory structure")
    args = parser.parse_args()
    
    page_numbers = parse_page_numbers(args.page_numbers)
    file_paths = construct_file_paths(args.catalog, args.schema, args.run_id, page_numbers)
    spark = get_spark()
    raw_df = read_json_projects(spark, file_paths)
    full_snapshot = create_full_snapshot_from_df(raw_df)
    merge_into_delta_table(spark, full_snapshot, args.catalog, args.schema)
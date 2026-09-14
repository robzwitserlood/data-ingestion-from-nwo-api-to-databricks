import argparse
import json
from typing import List

from pyspark.sql.functions import explode, col, get_json_object, md5
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from delta.tables import DeltaTable
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession, DataFrame

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

def assert_unique_not_null_ids(df: DataFrame, id_col: str = 'project_id') -> DataFrame:
    """Raises DuplicateOrNullIdentifierError if id_col has any null or duplicate values.

    Names the count of null values and the count of duplicated values found.
    Returns df unchanged if the check passes.
    """
    null_count = df.filter(col(id_col).isNull()).count()
    duplicate_count = (
        df.groupBy(id_col)
          .count()
          .filter((col('count') > 1) & col(id_col).isNotNull())
          .count()
    )
    if null_count > 0 or duplicate_count > 0:
        raise DuplicateOrNullIdentifierError(
            f"Column '{id_col}' has {null_count} null value(s) and {duplicate_count} "
            f"duplicated value(s); expected all values to be unique and non-null"
        )
    return df

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
    return assert_unique_not_null_ids(snapshot, 'project_id').alias('full_snapshot')

def merge_into_delta_table(spark: SparkSession, full_snapshot: DataFrame, catalog: str, schema: str) -> None:
    """Merges the full snapshot DataFrame into the Delta table."""

    spark.catalog.setCurrentCatalog(catalogName=catalog)
    spark.catalog.setCurrentDatabase(dbName=schema)

    if spark.catalog.tableExists(tableName='nwo_projects'):
        target = DeltaTable.forName(spark, 'nwo_projects').alias('target')
        target \
            .merge(full_snapshot, 'target.project_id = full_snapshot.project_id') \
            .whenMatchedUpdate(
                condition='target.row_hash <> full_snapshot.row_hash',
                set={
                    'project_id': 'full_snapshot.project_id',
                    'project': 'full_snapshot.project',
                    'row_hash': 'full_snapshot.row_hash'
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
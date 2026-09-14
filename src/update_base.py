import argparse
from typing import Iterable, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, get_json_object, trim, when, lower, lit
from pyspark.sql.types import StringType
from databricks.connect import DatabricksSession

ColumnSpec = Tuple[str, str, str]

COLUMN_SPECS = [
    ("$.title", "title", "string"),
    ("$.department", "department", "string"),
    ("$.sub_department", "sub_department", "string"),
    ("$.reporting_year", "reporting_year", "int"),
    ("$.start_date", "start_date", "date"),
    ("$.end_date", "end_date", "date"),
    ("$.award_amount", "award_amount", "int"),
    ("$.summary_nl", "summary_nl", "string"),
    ("$.summary_en", "summary_en", "string"),
]


def get_spark() -> SparkSession:
  spark = DatabricksSession.builder.getOrCreate()
  return spark


def parse_project_columns(df: DataFrame, column_specs: Iterable[ColumnSpec]) -> DataFrame:
    """
    Extract project-level fields from the `project` JSON column using provided mapping.

    column_specs: iterable of tuples (json_path, alias, dtype)
      - json_path: JSONPath string used with get_json_object (e.g. '$.title')
      - alias: column name to create (e.g. 'title')
      - dtype: target data type as a string ('int', 'date', 'string', ...)

    Returns a DataFrame with 'project_id' plus one column per alias.
    """
    selects = [get_json_object(col("project"), json_path).alias(alias)
               for json_path, alias, _ in column_specs]
    selects = [col("project_id")] + selects
    return df.select(*selects)


def cleanse_missing_values(df: DataFrame) -> DataFrame:
    """
    Replace common missing-value sentinels in string columns with null.
    Handles: empty / whitespace-only, one-or-more hyphens (-, –, —), and tokens
    like 'n/a', 'na', 'none', 'null' (case-insensitive).
    """
    string_cols = {f.name for f in df.schema.fields if isinstance(f.dataType, StringType)}
    exprs = []
    for c in df.columns:
        if c in string_cols:
            t = trim(col(c))
            cond = (
                (t == "") |
                t.rlike(r'^[\-\u2013\u2014]+$') |               # hyphen / en/em dash sequences
                lower(t).isin("n/a", "na", "none", "null")      # common tokens
            )
            exprs.append(when(cond, lit(None)).otherwise(col(c)).alias(c))
        else:
            exprs.append(col(c))
    return df.select(*exprs)


def apply_types(df: DataFrame, column_specs: Iterable[ColumnSpec]) -> DataFrame:
    """
    Cast columns according to column_specs and return a typed projection.

    column_specs: iterable of tuples (json_path, alias, dtype)
      - json_path: JSONPath string used with get_json_object (e.g. '$.title')
      - alias: column name to create (e.g. 'title')
      - dtype: target data type as a string ('int', 'date', 'string', ...)
    """
    exprs = [col(alias).cast(dtype) for _, alias, dtype in column_specs]
    exprs = [col("project_id")] + exprs
    return df.select(*exprs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Transform raw NWO projects table: 
        parse JSON, cleanse missing values, and apply types."""
    )
    parser.add_argument("--catalog", required=True, type=str)
    parser.add_argument("--schema_from", required=True, type=str)
    parser.add_argument("--schema_to", required=True, type=str)
    args = parser.parse_args()

    spark = get_spark()

    # Read raw table
    raw = spark.read.table(f'{args.catalog}.{args.schema_from}.nwo_projects')

    # Transform: parse -> cleanse -> type
    parsed = parse_project_columns(raw, COLUMN_SPECS)
    cleansed = cleanse_missing_values(parsed)
    typed = apply_types(cleansed, COLUMN_SPECS)

    # Write to target table
    typed.write.mode('overwrite').saveAsTable(f'{args.catalog}.{args.schema_to}.nwo_projects')
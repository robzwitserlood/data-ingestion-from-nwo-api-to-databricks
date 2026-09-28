"""This file configures pytest."""

import os, sys, pathlib
from contextlib import contextmanager


try:
    from databricks.connect import DatabricksSession
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession
    import pytest
except ImportError:
    raise ImportError("Test dependencies not found.\n\nRun tests using 'uv run pytest'. See http://docs.astral.sh/uv to learn more about uv.")


def enable_fallback_compute():
    """Enable serverless compute if no compute is specified."""
    conf = WorkspaceClient().config
    if conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE"):
        return

    url = "https://docs.databricks.com/dev-tools/databricks-connect/cluster-config"
    print("☁️ no compute specified, falling back to serverless compute", file=sys.stderr)
    print(f"  see {url} for manual configuration", file=sys.stderr)

    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"


@contextmanager
def allow_stderr_output(config: pytest.Config):
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")
    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]):
    """Mark every test that (directly or via another fixture) uses `spark`."""
    for item in items:
        if "spark" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.spark)


@pytest.fixture(scope="session")
def spark(pytestconfig: pytest.Config) -> SparkSession:
    """Provide a SparkSession fixture for tests.

    Created lazily on first use, so tests that don't need Spark run without a
    Databricks connection (e.g. `uv run pytest -m "not spark"`).
    """
    with allow_stderr_output(pytestconfig):
        enable_fallback_compute()

        # For DB Connect 15+, validate version compatibility with the remote cluster.
        if hasattr(DatabricksSession.builder, "validateSession"):
            return DatabricksSession.builder.validateSession().getOrCreate()
        return DatabricksSession.builder.getOrCreate()

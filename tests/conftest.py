import os

import pytest


def pytest_collection_modifyitems(config, items):
    """Skip integration tests when VERTICA_HOST is not set."""
    if not os.environ.get("VERTICA_HOST"):
        skip = pytest.mark.skip(reason="VERTICA_HOST not set — skipping integration tests")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)

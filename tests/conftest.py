"""
Define fixtures and configuration
that can be reused throughout pytest without redefinition.
It also defines the configurations for pytest.
"""
import os
import pytest


# functions to mark slow tests and skip them.
def pytest_addoption(parser):
    """
    Parse pytest to read --slow in testing
    """
    parser.addoption(
        "--slow",
        action="store_true",
        default=False,
        help="run (slow) performance tests",
    )


def pytest_configure(config):
    """
    Configure markers that may be needed in our testing framework
    """
    config.addinivalue_line(
        "markers", "slow: mark test as a (potentially slow) performance test"
    )


def pytest_collection_modifyitems(config, items):
    """
    Modify items from config
    """
    if config.getoption("--slow"):
        return
    skip_perf = pytest.mark.skip(reason="need --slow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_perf)

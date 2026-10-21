"""Smoke test: every module imports on this Python"""

import importlib

import pytest

MODULES = [
    "retrieve",
    "sql",
    "upsert",
]


@pytest.mark.parametrize("name", MODULES)
def test_import(name):
    importlib.import_module(name)

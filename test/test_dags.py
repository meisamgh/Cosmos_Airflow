# tests/test_dags.py
import pytest
from airflow.models import DagBag

def test_dag_loading():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"

"""
Shared pytest setup for the DAG unit tests.

Adds the ``dags`` directory (the parent of this ``tests`` package) to
``sys.path`` so tests import modules the same way Airflow does at runtime
(``from utils.bucket import ...``), where ``PYTHONPATH`` is ``/opt/airflow/dags``.
"""

import os
import sys

_DAGS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)

"""
Make ``utils`` importable when pytest runs from ``airflow/`` or the repo root.

Inside the Airflow containers ``PYTHONPATH=/opt/airflow/dags`` already does
this; locally the DAG folder has to be put on ``sys.path`` explicitly.
"""

import os
import sys

DAGS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)

"""Make ``main`` importable whether pytest runs from /app (container) or api/ (host)."""

import os
import sys

API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

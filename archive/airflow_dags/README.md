# Archived Airflow DAGs

Retired DAGs kept for reference. This folder is outside `airflow/dags/`, so Airflow does not load them.

| File | Retired | Why |
|---|---|---|
| `openneuro_classify_benchmark.py` + `OpenNeuro_Datasets_BM.csv` | 2026-09-23 | Superseded by `reuse_classification_benchmark_test`. It only checked whether D3's citation edges contained 281 known OpenNeuro paper → dataset mentions (a mapping check; it never ran the classifier). The CSV is the intended OpenNeuro answer key for that benchmark's planned mapping stage. |

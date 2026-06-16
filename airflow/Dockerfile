FROM apache/airflow:3.1.5

USER root

# Install system dependencies 
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Build version stamp (git SHA), available to every Airflow container / DAG via
# os.getenv("GIT_SHA"). Defaults to "dev" locally.
ARG GIT_SHA=dev
ENV GIT_SHA=$GIT_SHA


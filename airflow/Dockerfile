FROM apache/airflow:3.1.5

USER root

# Install system dependencies. git is needed by pip to install
# paper-text-fetcher from GitHub (see requirements.txt).
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies as the airflow user (pip refuses to run as root
# in this image), then install the chromium browser that paper-text-fetcher's
# playwright extra drives. `--with-deps` apt-installs chromium's shared
# libraries, which needs root, so it runs in its own layer as root.
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

USER root
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright
RUN mkdir -p /opt/playwright \
    && python -m playwright install --with-deps chromium \
    && chown -R airflow:root /opt/playwright \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Build version stamp (git SHA), available to every Airflow container / DAG via
# os.getenv("GIT_SHA"). Defaults to "dev" locally.
ARG GIT_SHA=dev
ENV GIT_SHA=$GIT_SHA

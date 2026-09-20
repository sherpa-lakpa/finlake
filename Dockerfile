# Local development image with PySpark + DuckDB pre-installed, so trying
# execution.mode: pyspark and execution.mode: sql tables locally doesn't
# require installing a JVM (PySpark's biggest local setup friction point)
# or any other native dependency on your own machine. Not used for
# production -- production runs on an actual Databricks cluster, which
# already has Spark/Java provisioned; this image exists purely for local
# development and CI.
FROM python:3.11-slim

# PySpark requires a JVM. Java 17 matches what recent Databricks Runtime
# versions ship, keeping local behavior close to production's.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# Install dependencies first (better layer caching): copy only the files
# pip needs before the rest of the source, so `docker compose build` after
# an ordinary code change doesn't reinstall PySpark/DuckDB from scratch.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[dev,spark,duckdb]"

COPY . .

CMD ["bash"]

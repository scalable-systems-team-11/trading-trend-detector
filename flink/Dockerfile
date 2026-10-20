# Build from the repository root: docker build -f flink/Dockerfile .
# PyFlink 1.20 supports Python up to 3.11 and needs a JVM.
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless gcc g++ curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" /opt/java
ENV JAVA_HOME=/opt/java

WORKDIR /app
COPY flink/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kafka connector for Flink 1.20 (bundles the Kafka client).
RUN curl -fsSL -o /opt/flink-sql-connector-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar

COPY receiver/*.py receiver/
COPY flink/*.py flink/

# The Python worker processes that Flink starts must be able to import the job modules.
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
ENTRYPOINT ["python", "-m", "flink.job"]

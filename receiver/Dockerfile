FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./receiver/

ENV PYTHONUNBUFFERED=1
EXPOSE 9999

# The Kafka worker image reuses this one: entrypoint is overridden in docker-compose.yml.
ENTRYPOINT ["python", "-m", "receiver.receiver"]

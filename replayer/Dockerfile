FROM python:3.12-slim

WORKDIR /app
COPY replay.py .

# Show replay output/logs immediately in `docker compose logs`.
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "replay.py"]

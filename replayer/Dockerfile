FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY replay.py .

# Show replay output/logs immediately in `docker compose logs`.
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "replay.py"]

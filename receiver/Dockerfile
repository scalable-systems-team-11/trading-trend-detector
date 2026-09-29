FROM python:3.12-slim

WORKDIR /app
COPY receiver.py .

ENV PYTHONUNBUFFERED=1
EXPOSE 9999

ENTRYPOINT ["python", "receiver.py"]

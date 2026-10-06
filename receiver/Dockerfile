FROM python:3.12-slim

WORKDIR /app
COPY *.py ./receiver/

ENV PYTHONUNBUFFERED=1
EXPOSE 9999

ENTRYPOINT ["python", "-m", "receiver.receiver"]

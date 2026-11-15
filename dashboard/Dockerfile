FROM python:3.12-slim

# Standard library only: nothing to install.
WORKDIR /app
COPY __init__.py server.py ./dashboard/
COPY static ./dashboard/static/

ENV PYTHONUNBUFFERED=1
EXPOSE 8050
ENTRYPOINT ["python", "-m", "dashboard.server", "--host", "0.0.0.0"]

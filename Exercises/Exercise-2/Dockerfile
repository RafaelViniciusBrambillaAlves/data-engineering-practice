FROM python:3.9-slim

WORKDIR app
COPY . /app

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -r requirements.txt
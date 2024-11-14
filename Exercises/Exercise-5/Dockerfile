FROM python:3.11

WORKDIR app
COPY . /app

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -r requirements.txt && \
    python3 -m pip show pandas
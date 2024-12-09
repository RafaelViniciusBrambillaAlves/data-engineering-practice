FROM ubuntu:18.04

RUN apt-get update && \
    apt-get install -y vim software-properties-common python3.8 python3-pip libpq-dev build-essential libssl-dev libffi-dev python3-dev && \
    apt-get clean

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1 && \
    update-alternatives --set python3 /usr/bin/python3.8 && \
    python3 --version

RUN python3 -m pip install --upgrade pip

WORKDIR /app

COPY . /app

RUN pip3 install -r requirements.txt

CMD ["python3", "main.py"]

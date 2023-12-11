FROM ubuntu:18.04

RUN apt-get update && \
    apt-get install -y vim software-properties-common python3.8 python3-pip libpq-dev build-essential libssl-dev libffi-dev python3-dev && \
    apt-get clean

WORKDIR lab6
COPY . /lab6

RUN pip3 install -r requirements.txt

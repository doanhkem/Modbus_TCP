# syntax=docker/dockerfile:1
FROM python:3.10-alpine
ENV PYTHONUNBUFFERED=1
RUN mkdir /etc/modbus/
WORKDIR /etc/modbus/
RUN apk add build-base && \
  apk add git && \
  git clone https://github.com/tienvinhle/IOTGateway-modbus.git /etc/modbus/ && \
  pip install requirements.txt && \
  rm -rf /var/cache/apk/*
CMD ["python", "appConnectivity.py"]
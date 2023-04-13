# Do not use this image if deploying fastapi via K8s or Openshift (use Python base image instead)
# Source: https://github.com/tiangolo/uvicorn-gunicorn-fastapi-docker
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.11
# FROM python:3.11

WORKDIR /wine

COPY ./requirements.txt /wine/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /wine/requirements.txt

COPY ./api /wine/api
COPY ./schemas /wine/schemas

EXPOSE 8000
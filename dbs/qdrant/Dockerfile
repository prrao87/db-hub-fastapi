FROM python:3.10-slim-bullseye

WORKDIR /wine

COPY ./requirements-docker.txt /wine/requirements-docker.txt

RUN pip install --no-cache-dir -U pip wheel setuptools
RUN pip install --no-cache-dir -r /wine/requirements-docker.txt

COPY ./api /wine/api
COPY ./schemas /wine/schemas
COPY ./scripts/onnx_models /wine/scripts/onnx_models

EXPOSE 8000
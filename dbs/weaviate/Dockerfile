FROM python:3.10-slim-bullseye

WORKDIR /wine

COPY ./requirements.txt /wine/requirements.txt

RUN pip install --no-cache-dir -U pip wheel setuptools
RUN pip install --no-cache-dir -r /wine/requirements.txt

COPY ./api /wine/api
COPY ./schemas /wine/schemas

EXPOSE 8000
FROM python:3.11-slim-bullseye

WORKDIR /wine

COPY ./requirements.txt /wine/requirements.txt

RUN pip install --no-cache-dir -U pip wheel setuptools
RUN pip install --no-cache-dir -r /wine/requirements.txt

COPY ./winemag /wine/winemag
COPY ./api /wine/api
COPY ./schemas /wine/schemas

EXPOSE 8000
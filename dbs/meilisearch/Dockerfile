FROM python:3.11-slim-bullseye

WORKDIR /wine

COPY ./requirements.txt /wine/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /wine/requirements.txt

COPY ./api /wine/api

EXPOSE 8000
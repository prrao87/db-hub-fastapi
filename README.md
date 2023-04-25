# Async database transactions and FastAPI

This repo aims to provide working code and reproducible setups for asynchronous data ingestion and querying from numerous databases via Python. Wherever possible, data is ingested to a database via their supported async Python drivers, and the data is also queried in async fashion on top of FastAPI endpoints.

Example code is provided for numerous databases, along with FastAPI docker deployments that allow you to easily supply complex query results to downstream applications.

#### Currently implemented
* Neo4j
* Elasticsearch
* Meilisearch
* Qdrant
* Weviate

## Goals

The primary aim is to compare the data ingestion and query performance of various databases that can be used for a host of downstream use cases. Two use cases are of particular interest:

1. We may want to expose (potentially sensitive) data to downstream client applications, so building an API on top of the database can be a very useful tool to share the data in a controlled manner

2. Databases or data stores in general can be important "sources of truth" for contextual querying via LLMs like ChatGPT, allowing us to ground our model's results with factual data. APIs allow us to add another layer to simplify querying a host of backends in a way that doesn't rely on the LLM learning a specific query language.

In general, it's useful to have a clean, efficient and reproducible workflow to experiment with each database in question.


## Pre-requisites

Install Docker and the latest version of Python (3.11+), as recent syntactic improvements in Python are extensively utilized in the code provided.

## About the dataset

The [dataset provided](https://github.com/prrao87/async-db-fastapi/tree/main/data) in this repo is a formatted version of the version obtained from Kaggle datasets. Full credit is due to [the original author](https://www.kaggle.com/zynicide) via Kaggle for curating this dataset.

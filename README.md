# DBHub

## Boilerplate for async ingestion and querying of DBs

This repo aims to provide working code and reproducible setups for bulk data ingestion and querying from numerous databases via their Python clients. Wherever possible, async database client APIs are utilized for data ingestion. The query interface to the data is exposed via async FastAPI endpoints. To enable reproducibility across environments, Dockerfiles are provided as well.

The `docker-compose.yml` does the following:
1. Set up a local DB server in a container
2. Set up local volume mounts to persist the data
3. Set up a FastAPI server in another container
4. Set up a network bridge such that the DB server can be accessed from the FastAPI server
5. Tear down all the containers once development and testing is complete

### Currently implemented
* Neo4j
* Elasticsearch
* Meilisearch
* Qdrant
* Weaviate
* LanceDB

## Goals

The main goals of this repo are explained as follows.

1. **Ease of setup**: There are tons of databases and client APIs out there, so it's useful to have a clean, efficient and reproducible workflow to experiment with a range of datasets, as well as databases for the problem at hand.

2. **Ease of distribution**: We may want to expose (potentially sensitive) data to downstream client applications, so building an API on top of the database can be a very useful tool to share the data in a controlled manner

3. **Ease of testing advanced use cases**: Search databases (either full-text keyword search or vector DBs) can be important "sources of truth" for contextual querying via LLMs like ChatGPT, allowing us to ground our model's results with factual data.


## Pre-requisites

* Python 3.10+
* Docker
* A passion to learn more about and experiment with databases!

# Async database transactions and FastAPI

This repo aims to provide working code and reproducible setups for asynchronous data ingestion and querying from numerous databases via Python. Wherever possible, data is ingested to a database via their supported async Python drivers, and the data is also queried in async fashion on top of FastAPI endpoints.

Example code for the following databases are provided, along with a FastAPI deployment that allow you to query the database in question:

* Neo4j

#### 🚧 Coming soon

* Elasticsearch
* MongoDB
* Qdrant
* Weviate


## Goals

The primary aim is to compare the data ingestion and query performance of various databases (mostly NoSQL) that can be used for a host of downstream use cases. In particular, many NoSQL databases can form the backend of upcoming automated systems that provide ChatGPT and other LLMs contextual querying via natural language and short/long-term memory. As such, it's useful to have a clean, efficient and reproducible workflow to experiment with each database in question.

Vector DBs are particularly interesting as the backbone of contextual search applications, more on them soon!

## Pre-requisites

Install Docker and the latest version of Python (3.11+), as recent syntactic improvements in Python are extensively utilized in the code provided.

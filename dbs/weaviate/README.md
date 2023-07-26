# Weaviate

[Weaviate](https://weaviate.io/) is an ML-first vector search database written in Go. It allows users to store data objects and vector embeddings and scale to billions of objects, allowing for sub-millisecond searches. The primary use case for a vector database is to retrieve results that are most semantically similar to the input natural language query. The semantic similarity is obtained by comparing the sentence embeddings (which are n-dimensional vectors) between the input query and the data stored in the database. Most vector DBs, including Weaviate, store both the metadata (as JSON) and the sentence embeddings of text on which we want to search (as vectors), allowing us to perform much more flexible searches than keyword-only search databases. In the case of Weaviate, it even allows hybrid searches, giving developers the flexibility to decide what search methods work best on the data at hand.

Code is provided for ingesting the wine reviews dataset into Weaviate. In addition, a query API written in FastAPI is also provided that allows a user to query available endpoints. As always in FastAPI, documentation is available via OpenAPI (http://localhost:8004/docs).

* Unlike "normal" databases, in a vector DB, the vectorization process is the biggest bottleneck, and because a lot of vector DBs are relatively new, they do not yet support async indexing (although they might, soon).
  * It doesn't make sense to focus on async requests for vector DBs at present -- rather, it makes more sense to focus on speeding up the vectorization process
* [Pydantic](https://docs.pydantic.dev) is used for schema validation, both prior to data ingestion and during API request handling
* For ease of reproducibility during development, the whole setup is orchestrated and deployed via docker

## Setup

Note that this code base has been tested in Python 3.10, and requires a minimum of Python 3.10 to work. Install dependencies via `requirements.txt`.

```sh
# Setup the environment for the first time
python -m venv weaviate_venv  # python -> python 3.10

# Activate the environment (for subsequent runs)
source weaviate_venv/bin/activate

python -m pip install -r requirements.txt
```

--- 

## Step 1: Set up containers

Docker compose files are provided, which start a persistent-volume Weaviate database with credentials specified in `.env`. The `weaviate` variable in the environment file under the `fastapi` service indicates that we are opening up the database service to FastAPI (running as a separate service, in a separate container) downstream. Both containers can communicate with one another with the common network that they share, on the exact port numbers specified.

The database and API services can be restarted at any time for maintenance and updates by simply running the `docker restart <container_name>` command.

**💡 Note:** The setup shown here would not be ideal in production, as there are other details related to security and scalability that are not addressed via simple docker, but, this is a good starting point to begin experimenting!

### Use `sbert` model

If using the `sbert` model [from the sentence-transformers repo](https://www.sbert.net/) directly, use the provided `docker-compose.yml` to initiate separate containers, one that runs Weaviate, and another one that serves as an API on top of the database.

**⚠️ Note**: This approach will attempt to run `sbert` on a GPU if available, and if not, on CPU (while utilizing all CPU cores).

```
docker compose -f docker-compose.yml up -d
```
Tear down the services using the following command.

```
docker compose -f docker-compose.yml down
```

## Step 2: Ingest the data

We ingest both the JSON data for full-text search and filtering, as well as the sentence embedding vectors for similarity search into Weaviate. For this dataset, it's reasonable to expect that a simple concatenation of fields like `title`, `country`, `province`, `variety` and `description` would result in a useful vector that can be compared against a search query, also vectorized in the same embedding space.

As an example, consider the following data snippet form the `data/` directory in this repo:

```json
"variety": "Red Blend",
"country": "Italy",
"province": "Tuscany",
"title": "Castello San Donato in Perano 2009 Riserva  (Chianti Classico)",
"description": "Made from a blend of 85% Sangiovese and 15% Merlot, this ripe wine delivers soft plum, black currants, clove and cracked pepper sensations accented with coffee and espresso notes. A backbone of firm tannins give structure. Drink now through 2019."
```

The above fields are concatenated for vectorization, and then indexed along with the data to Weaviate.


### Choice of embedding model

[SentenceTransformers](https://www.sbert.net/) is a Python framework for a range of sentence and text embeddings. It results from extensive work on fine-tuning BERT to work well on semantic similarity tasks using Siamese BERT networks, where the model is trained to predict the similarity between sentence pairs. The original work is [described here](https://arxiv.org/abs/1908.10084).

#### Why use sentence transformers?

Although larger and more powerful text embedding models exist (such as [OpenAI embeddings](https://platform.openai.com/docs/guides/embeddings)), they can become really expensive as they are not free, and charge per token of text. SentenceTransformers are free and open-source, and have been optimized for years for performance, both to utilize all CPU cores and for reduced size while maintaining performance. A full list of sentence transformer models [is in the project page](https://www.sbert.net/docs/pretrained_models.html).

For this work, it makes sense to use among the fastest models in this list, which is the `multi-qa-MiniLM-L6-cos-v1` **uncased** model. As per the docs, it was tuned for semantic search and question answering, and generates sentence embeddings for single sentences or paragraphs up to a maximum sequence length of 512. It was trained on 215M question answer pairs from various sources. Compared to the more general-purpose `all-MiniLM-L6-v2` model, it shows slightly improved performance on semantic search tasks while offering a similar level of performance. [See the sbert docs](https://www.sbert.net/docs/pretrained_models.html) for more details on performance comparisons between the various pretrained models.

### Run data loader

Data is ingested into the Weaviate database through the scripts in the `scripts` directly. The scripts validate the input JSON data via [Pydantic](https://docs.pydantic.dev), and then index both the JSON data and the vectors to Weaviate using the [Weaviate Python client](https://github.com/weaviate/weaviate-python-client).

As mentioned before, the fields `variety`, `country`, `province`, `title` and `description` are concatenated, vectorized, and then indexed to Weaviate.

If running on a Macbook or a machine without a GPU, it's possible to generate sentence embeddings using the original `sbert` model as per the `EMBEDDING_MODEL_CHECKPOINT` variable in the `.env` file.

```sh
cd scripts
python bulk_index_sbert.py
```

Depending on the CPU on your machine, this may take a while. On a 2022 M2 Macbook Pro, vectorizing and bulk-indexing ~130k records took about 25 minutes. When tested on an AWS EC2 T2 medium instance, the same process took just over an hour.

## Step 3: Test API

Once the data has been successfully loaded into Weaviate and the containers are up and running, we can test out a search query via an HTTP request as follows.

```sh
curl -X 'GET' \
  'http://0.0.0.0:8004/wine/search?terms=tuscany%20red'
```

This cURL request passes the search terms "**tuscany red**", along with the country "Italy" and a maximum price of "100" to the `/wine/search/` endpoint, which is then parsed into a working filter query to Weaviate by the FastAPI backend. The query runs and retrieves results that are semantically similar to the input query for red Tuscan wines, and, if the setup was done correctly, we should see the following response:

```json
[
  {
    "wineID": 55924,
    "country": "Italy",
    "province": "Tuscany",
    "title": "Col d'Orcia 2011 Spezieri Red (Toscana)",
    "description": "This easy going blended red from Tuscany opens with bright cherry and blackberry aromas against a backdrop of bitter almond and a touch of Indian spice. The fresh acidity makes this a perfect pasta wine.",
    "points": 87,
    "price": 17,
    "variety": "Red Blend",
    "winery": "Col d'Orcia"
  },
  {
    "wineID": 5525,
    "country": "Italy",
    "province": "Tuscany",
    "title": "Grati 2014 Red (Toscana)",
    "description": "Here's a frail red that offers weak sensations of sour cherry and strawberry alongside zesty acidity and fleeting, gritty tannins. It's short and dilute.",
    "points": 83,
    "price": 11,
    "variety": "Red Blend",
    "winery": "Grati"
  },
  {
    "wineID": 40960,
    "country": "Italy",
    "province": "Tuscany",
    "title": "Fattoria di Grignano 2011 Pietramaggio Red (Toscana)",
    "description": "Here's a simple but well made red from Tuscany that has floral aromas of violet and rose with berry notes. The palate offers bright cherry, red currant and a touch of spice. Pair this with pasta dishes or grilled vegetables.",
    "points": 86,
    "price": 11,
    "variety": "Red Blend",
    "winery": "Fattoria di Grignano"
  }
]
Response headers
```

Not bad! This example correctly returns some highly rated Tuscan red wines form Italy along with their price. More specific search queries, such as low/high acidity, or flavour profiles of wines can also be entered to get more relevant results by country.

## Step 4: Extend the API

The API can be easily extended with the provided structure.

- The `schemas` directory houses the Pydantic schemas, both for the data input as well as for the endpoint outputs
  - As the data model gets more complex, we can add more files and separate the ingestion logic from the API logic here
- The `api/routers` directory contains the endpoint routes so that we can provide additional endpoint that answer more business questions
  - For e.g.: "What are the top rated wines from Argentina?"
  - In general, it makes sense to organize specific business use cases into their own router files
- The `api/main.py` file collects all the routes and schemas to run the API


#### Existing endpoints

As an example, some search endpoints are implemented and can be accessed via the API at the following URLs.

```
GET
/wine/search
Semantic similarity search
```

```
GET
/wine/search_by_country
Semantic similarity search for wines by country
```

```
GET
/wine/search_by_filters
Semantic similarity search for wines by country, price and points (review ratings)
```

```
GET
/wine/count_by_country
Get counts of wines by country
```

```
GET
/wine/count_by_filters
Get counts of wines by country, price and points (review ratings)
```
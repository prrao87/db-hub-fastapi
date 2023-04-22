# Qdrant

[Qdrant](https://qdrant.tech/) is a vector database and vector similarity search engine written in Rust. The primary use case for a vector database is to answer business questions that involve connected data.

* Which wines from Chile were tasted by at least two different tasters?
* What are the top-rated wines from Italy that share their variety with my favourite ones from Portugal?

Code is provided for ingesting the wine reviews dataset into Qdrant in an async fashion. In addition, a query API written in FastAPI is also provided that allows a user to query available endpoints. As always in FastAPI, documentation is available via OpenAPI (http://localhost:8000/docs).

* All code (wherever possible) is async
* [Pydantic](https://docs.pydantic.dev) is used for schema validation, both prior to data ingestion and during API request handling
  * The same schema is used for data ingestion and for the API, so there is only one source of truth regarding how the data is handled
* For ease of reproducibility, the whole setup is orchestrated and deployed via docker

## Setup

Note that this code base has been tested in Python 3.10, and requires a minimum of Python 3.10 to work. Install dependencies via `requirements.txt`.

```sh
# Setup the environment for the first time
python -m venv qdrant_venv  # python -> python 3.10

# Activate the environment (for subsequent runs)
source qdrant_venv/bin/activate

python -m pip install -r requirements.txt
```

--- 

## Step 1: Set up containers

Use the provided `docker-compose.yml` to initiate separate containers, one that runs Qdrant, and another one that serves as an API on top of the database.

```
docker compose up -d
```

This compose file starts a persistent-volume Qdrant database with credentials specified in `.env`. The `qdrant` variable in the environment file indicates that we are opening up the database service to a FastAPI server (running as a separate service, in a separate container) downstream. Both containers can communicate with one another with the common network that they share, on the exact port numbers specified.

The services can be stopped at any time for maintenance and updates.

```
docker compose down
```

**Note:** The setup shown here would not be ideal in production, as there are other details related to security and scalability that are not addressed via simple docker, but, this is a good starting point to begin experimenting!


## Step 1: Ingest the data

Because Qdrant is a vector database, we ingest not only the wine reviews JSON blobs for each item, but also vectors (i.e., sentence embeddings) for the fields on which we want to perform a semantic similarity search. For this dataset, it's reasonable to expect that a simple concatenation of fields like `title`, `variety` and `description` would result in a useful sentence embedding that can be compared against a search query (which is also converted to a vector during query time).

As an example, consider the following data snippet form the `data/` directory in this repo:

```json
"title": "Castello San Donato in Perano 2009 Riserva  (Chianti Classico)",
"description": "Made from a blend of 85% Sangiovese and 15% Merlot, this ripe wine delivers soft plum, black currants, clove and cracked pepper sensations accented with coffee and espresso notes. A backbone of firm tannins give structure. Drink now through 2019.",
"variety": "Red Blend"
```

### Choice of embedding model

[SentenceTransformers](https://www.sbert.net/) is a Python framework for a range of sentence and text embeddings. It results from extensive work on fine-tuning BERT to work well on semantic similarity tasks using Siamese BERT networks, where the model is trained to predict the similarity between sentence pairs. The original work is [described here](https://arxiv.org/abs/1908.10084).

#### Why use sentence transformers?

Although larger and more powerful text embedding models exist (such as [OpenAI embeddings](https://platform.openai.com/docs/guides/embeddings)), they can become really expensive as they are not free, and charge per token of text they generate vectors for. SentenceTransformers are free and open-source, and have been optimized for years for performance (to utilize all CPU cores) as well as accuracy. A full list of sentence transformer models [is in their project page](https://www.sbert.net/docs/pretrained_models.html).

For this work, it makes sense to use among the fastest models in this list, which is the `multi-qa-MiniLM-L6-cos-v1` **uncased** model. As the name suggests, it was tuned for semantic search and question answering, and generates sentence embeddings for single sentences or paragraphs up to a maximum sequence length of 512. It was trained on 215M question answer pairs from various sources. Compared to the more general-purpose `all-MiniLM-L6-v2` model, it shows slightly improved performance on semantic search tasks while offering a similar level of performance. [See the sbert docs](https://www.sbert.net/docs/pretrained_models.html) for more details on performance comparisons between the various pretrained models.


### Run data loader

Data is ingested into the Qdrant database through the scripts in the `scripts` directly.

```sh
cd scripts
python bulk_index_sbert.py
```

This script validates the input JSON data via [Pydantic](https://docs.pydantic.dev), and then indexes them to Qdrant using the [Qdrant Python client](https://github.com/qdrant/qdrant-client).

We simply concatenate the key fields that contain useful information about each wine, and vectorize them prior to indexing them to the database.


## Step 3: Test API

Once the data has been successfully loaded into Qdrant and the containers are up and running, we can test out a search query via an HTTP request as follows.

```sh
curl -X 'GET' \
  'http://localhost:8000/wine/search?terms=tuscany%20red&max_price=50'
```

This cURL request passes the search terms "**tuscany red**" to the `/wine/search/` endpoint, which is then parsed into a working Cypher query by the FastAPI backend. The query runs and retrieves results from a full text search index (that looks for these keywords in the wine's title and description), and, if the setup was done correctly, we should see the following response:

```json
[
    {
        "wineID": 66393,
        "country": "Italy",
        "title": "Capezzana 1999 Ghiaie Della Furba Red (Tuscany)",
        "description": "Very much a baby, this is one big, bold, burly Cab-Merlot-Syrah blend that's filled to the brim with extracted plum fruit, bitter chocolate and earth. It takes a long time in the glass for it to lose its youthful, funky aromatics, and on the palate things are still a bit scattered. But in due time things will settle and integrate",
        "points": 90,
        "price": 49,
        "variety": "Red Blend",
        "winery": "Capezzana"
    },
    {
        "wineID": 40960,
        "country": "Italy",
        "title": "Fattoria di Grignano 2011 Pietramaggio Red (Toscana)",
        "description": "Here's a simple but well made red from Tuscany that has floral aromas of violet and rose with berry notes. The palate offers bright cherry, red currant and a touch of spice. Pair this with pasta dishes or grilled vegetables.",
        "points": 86,
        "price": 11,
        "variety": "Red Blend",
        "winery": "Fattoria di Grignano"
    },
    {
        "wineID": 73595,
        "country": "Italy",
        "title": "I Giusti e Zanza 2011 Belcore Red (Toscana)",
        "description": "With aromas of violet, tilled soil and red berries, this blend of Sangiovese and Merlot recalls sunny Tuscany. It's loaded with wild cherry flavors accented by white pepper, cinnamon and vanilla. The palate is uplifted by vibrant acidity and fine tannins.",
        "points": 89,
        "price": 27,
        "variety": "Red Blend",
        "winery": "I Giusti e Zanza"
    }
]
```

Not bad! This example correctly returns some highly rated Tuscan red wines along with their price and country of origin (obviously, Italy in this case).

### Step 4: Extend the API

The API can be easily extended with the provided structure.

- The `schemas` directory houses the Pydantic schemas, both for the data input as well as for the endpoint outputs
  - As the data model gets more complex, we can add more files and separate the ingestion logic from the API logic here
- The `api/routers` directory contains the endpoint routes so that we can provide additional endpoint that answer more business questions
  - For e.g.: "What are the top rated wines from Argentina?"
  - In general, it makes sense to organize specific business use cases into their own router files
- The `api/main.py` file collects all the routes and schemas to run the API


#### Existing endpoints

So far, the following endpoints that help answer interesting questions have been implemented.

```
GET
/wine/search
Semantic similarity search
```

More to come soon!


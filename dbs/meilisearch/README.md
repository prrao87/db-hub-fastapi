# Meilisearch

[Meilisearch](https://www.meilisearch.com/docs/learn/what_is_meilisearch/overview) is a fast, responsive RESTful search engine database [built in Rust](https://github.com/meilisearch/meilisearch). The primary use case for Meilisearch is to answer business questions that involve typo-tolerant and near-instant searching on keywords or phrases, all enabled by its efficient indexing and storage techniques.

Code is provided for ingesting the wine reviews dataset into Meilisearch in an async fashion. In addition, a query API written in FastAPI is also provided that allows a user to query available endpoints. As always in FastAPI, documentation is available via OpenAPI.

* All code (wherever possible) is async
* [Pydantic](https://docs.pydantic.dev) is used for schema validation, both prior to data ingestion and during API request handling
  * The same schema is used for data ingestion and for the API, so there is only one source of truth regarding how the data is handled
* For ease of reproducibility, the whole setup is orchestrated and deployed via docker

## Setup

Note that this code base has been tested in Python 3.11, and requires a minimum of Python 3.10 to work. Install dependencies via `requirements.txt`.

```sh
# Setup the environment for the first time
python -m venv meili_venv  # python -> python 3.10+

# Activate the environment (for subsequent runs)
source meili_venv/bin/activate

python -m pip install -r requirements.txt

```

--- 

## Step 1: Set up containers

Use the provided `docker-compose.yml` to initiate separate containers, one that runs Meilisearch, and another one that serves as an API on top of the database.

```
docker compose up -d
```

This compose file starts a persistent-volume Meilisearch database with credentials specified in `.env`. The `meilisearch` service variable in the environment file indicates that we are opening up the database service to a FastAPI server (running as a separate service, in a separate container) downstream. Both containers can communicate with one another with the common network that they share, on the exact port numbers specified.

The services can be stopped at any time for maintenance and updates.

```
docker compose down
```

**Note:** The setup shown here would not be ideal in production, as there are other details related to security and scalability that are not addressed via simple docker, but, this is a good starting point to begin experimenting!


## Step 1: Ingest the data

The first step is to ingest the wine reviews dataset into Meilisearch. Data is asynchronously ingested into the Meilisearch database through the scripts in the `scripts` directory.

```sh
cd scripts
python bulk_index.py
```

* This script first sets important items like which fields are searchable, filterable and sortable:
  * To speed up indexing, Meilisearch allows us to explicitly specify which fields are searchable, filterable and sortable
  * Choosing these fields carefully can really help speeding up indexing a large dataset, of the order of $10^5-10^6$ records
* The script then validates the input JSON data via [Pydantic](https://docs.pydantic.dev) and asynchronously indexes them into the database using the [`meilisearch-python-async` client](https://github.com/sanders41/meilisearch-python-async) for fastest performance
  * The third-party async Python client is chosen over the [official client](https://github.com/meilisearch/meilisearch-python) (for now, sync) for Meilisearch, as the goal is to provide an async-compatible API via FastAPI


## Step 3: Test API

Once the data has been successfully loaded into Meilisearch and the containers are up and running, we can test out a search query via an HTTP request as follows.

```sh
curl -X 'GET' \
  'http://localhost:8003/wine/search?terms=tuscany%20red'
```

This cURL request passes the search terms "**tuscany red**" to the `/wine/search/` endpoint, which is then parsed into a working Meilisearch JSON query by the FastAPI backend. The query runs and retrieves results from the database (that looks for these keywords in the wine's title, description and variety fields), and, if the setup was done correctly, we should see the following response:

```json
[
    {
        "id": 22170,
        "country": "Italy",
        "title": "Kirkland Signature 2004 Tuscany Red (Toscana)",
        "description": "Here is a masculine and robust blend of Sangiovese, Cab Sauvignon and Merlot that exhibits thick concentration and aromas of exotic spices, cherry, prune, plum, vanilla and Amaretto. The nose is gorgeous but the mouthfeel is less convincing, with firm tannins.",
        "points": 87,
        "price": 20.0,
        "variety": "Red Blend",
        "winery": "Kirkland Signature"
    },
    {
        "id": 55924,
        "country": "Italy",
        "title": "Col d'Orcia 2011 Spezieri Red (Toscana)",
        "description": "This easy going blended red from Tuscany opens with bright cherry and blackberry aromas against a backdrop of bitter almond and a touch of Indian spice. The fresh acidity makes this a perfect pasta wine.",
        "points": 87,
        "price": 17.0,
        "variety": "Red Blend",
        "winery": "Col d'Orcia"
    },
    {
        "id": 40960,
        "country": "Italy",
        "title": "Fattoria di Grignano 2011 Pietramaggio Red (Toscana)",
        "description": "Here's a simple but well made red from Tuscany that has floral aromas of violet and rose with berry notes. The palate offers bright cherry, red currant and a touch of spice. Pair this with pasta dishes or grilled vegetables.",
        "points": 86,
        "price": 11.0,
        "variety": "Red Blend",
        "winery": "Fattoria di Grignano"
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
Search By Keywords
```

```
GET
/wine/top_by_country
Top By Country
```

```
GET
/wine/top_by_province
Top By Province
```

Run the FastAPI app in a docker container to explore them!

---

### 💡 Limitations of Meilisearch

Because Meilisearch was designed from the ground up to be a near-instant search data store, it does not have great support for aggregations or analytics, which are features we might be used to from other NoSQL databases like ElasticSearch and MongoDB. More info on this is provided in [this excellent blog post](https://blog.meilisearch.com/why-should-you-use-meilisearch-over-elasticsearch/) by the Meilisearch creators themselves.

As stated in that blog post by Meilisearch :

> Meilisearch is not made to search through billions of large text files or parse complex queries. This kind of searching power would require a higher degree of complexity and lead to slower search experiences, which runs against our instant search philosophy. For those purposes, look no further than Elasticsearch; it’s an excellent solution for companies with the necessary resources, whether that be the financial means to hire consultants or the time and money required to implement it themselves.

**Bottom Line:** If your goal is to run analytics on your unstructured data, or more complex queries than string-based information retrieval, then, maybe Meilisearch isn't the best choice -- stick to more established alternatives like MongoDB or ElasticSearch that were designed for much more versatile use cases.

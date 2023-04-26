# Elasticsearch

[Elasticsearch](https://www.elastic.co/what-is/elasticsearch) is a distributed search and analytics engine for various kinds of structured and unstructured data. The primary use case for Elasticsearch is answer business questions that involve searching and retrieving information on full text, such as descriptions and titles.

Code is provided for ingesting the wine reviews dataset into Elasticsearch in an async fashion. In addition, a query API written in FastAPI is also provided that allows a user to query available endpoints. As always in FastAPI, documentation is available via OpenAPI (http://localhost:8000/docs).

* All code (wherever possible) is async
* [Pydantic](https://docs.pydantic.dev) is used for schema validation, both prior to data ingestion and during API request handling
  * The same schema is used for data ingestion and for the API, so there is only one source of truth regarding how the data is handled
* For ease of reproducibility, the whole setup is orchestrated and deployed via docker

## Setup

Note that this code base has been tested in Python 3.11, and requires a minimum of Python 3.10 to work. Install dependencies via `requirements.txt`.

```sh
# Setup the environment for the first time
python -m venv elastic_venv  # python -> python 3.10+

# Activate the environment (for subsequent runs)
source elastic_venv/bin/activate

python -m pip install -r requirements.txt
```

--- 

## Step 1: Set up containers

Use the provided `docker-compose.yml` to initiate separate containers, one that run Elasticsearch, and another one that serves as an API on top of the database.

```
docker compose up -d
```

This compose file starts a persistent-volume Elasticsearch database with credentials specified in `.env`. The `elasticsearch` service variable in the environment file indicates that we are opening up the database service to a FastAPI server (running as a separate service, in a separate container) downstream. Both containers can communicate with one another with the common network that they share, on the exact port numbers specified.

The services can be stopped at any time for maintenance and updates.

```
docker compose down
```

**Note:** The setup shown here would not be ideal in production, as there are other details related to security and scalability that are not addressed via simple docker, but, this is a good starting point to begin experimenting!


## Step 2: Ingest the data

The first step is to ingest the wine reviews dataset into Elasticsearch. Data is asynchronously ingested into the Elasticsearch database through the scripts in the `scripts` directory.

```sh
cd scripts
python bulk_index.py
```

* This script first checks the database for a mapping (that tells Elasticsearch what fields to analyze and how to index them). Each index is attached to an alias, "wines", which is used to reference all the operations downstream
  * If no existing index or alias is found, new ones are created
* The script then validates the input JSON data via [Pydantic](https://docs.pydantic.dev) and asynchronously indexes them into the database using the [`AsyncElasticsearch` client](https://elasticsearch-py.readthedocs.io/en/v8.7.0/async.html) for fastest performance


## Step 3: Test API

Once the data has been successfully loaded into Elasticsearch and the containers are up and running, we can test out a search query via an HTTP request as follows.

```sh
curl -X 'GET' \
  'http://localhost:8000/wine/search?terms=tuscany%20red'
```

This cURL request passes the search terms "**tuscany red**" to the `/wine/search/` endpoint, which is then parsed into a working Elasticsearch JSON query by the FastAPI backend. The query runs and retrieves results from the database (that looks for these keywords in the wine's title, description and variety fields), and, if the setup was done correctly, we should see the following response:

```json
[
    {
        "id": 109409,
        "country": "Italy",
        "title": "Castello Banfi 2007 Excelsus Red (Toscana)",
        "description": "The 2007 Excelsus is a gorgeous super Tuscan expression (with Cabernet Sauvignon and Merlot) that shows quality and superior fruit on all levels. Castello Banfi has really hit a home run with this vintage. You'll encounter persuasive aromas of cassis, blackberry, chocolate, tobacco, curry leaf and deep renderings of exotic spice. The wine's texture is exceedingly smooth, rich and long lasting.",
        "points": 97,
        "price": 81.0,
        "variety": "Red Blend",
        "winery": "Castello Banfi"
    },
    {
        "id": 21079,
        "country": "Italy",
        "title": "Marchesi Antinori 2010 Solaia Red (Toscana)",
        "description": "Already one of Italy's most iconic bottlings, this gorgeous 2010 is already a classic. Its complex and intense bouquet unfolds with ripe blackberries, violets, leather, thyme and balsamic herbs. The palate shows structure, poise and complexity, delivering rich black currants, black cherry, licorice, mint and menthol notes alongside assertive but polished tannins and vibrant energy. This wine will age and develop for decades. Drink 2018–2040.",
        "points": 97,
        "price": 325.0,
        "variety": "Red Blend",
        "winery": "Marchesi Antinori"
    },
    {
        "id": 35520,
        "country": "Italy",
        "title": "Marchesi Antinori 2012 Solaia Red (Toscana)",
        "description": "This stunning expression of Solaia opens with ample aromas of exotic spices, tilled soil, mature black-skinned fruit and an underlying whiff of fragrant blue flowers. The vibrant, elegantly structured palate doles out high-toned black cherry, ripe blackberry, white pepper, cinnamon, clove and Mediterranean herbs alongside a backbone of firm, polished tannins and bright acidity. Drink 2017–2032.",
        "points": 97,
        "price": 325.0,
        "variety": "Red Blend",
        "winery": "Marchesi Antinori"
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


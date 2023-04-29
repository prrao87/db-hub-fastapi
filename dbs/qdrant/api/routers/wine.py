from fastapi import APIRouter, HTTPException, Query, Request
from qdrant_client.http import models

from schemas.retriever import CountByCountry, SimilaritySearch

wine_router = APIRouter()


# --- Routes ---


@wine_router.get(
    "/search",
    response_model=list[SimilaritySearch],
    response_description="Search for wines via semantically similar terms",
)
def search_by_similarity(
    request: Request,
    terms: str = Query(
        description="Specify terms to search for in the variety, title and description"
    ),
) -> list[SimilaritySearch] | None:
    COLLECTION = "wines"
    result = _search_by_similarity(request, COLLECTION, terms)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided terms '{terms}' found in database - please try again",
        )
    return result


@wine_router.get(
    "/search_by_country",
    response_model=list[SimilaritySearch],
    response_description="Search for wines via semantically similar terms from a particular country",
)
def search_by_similarity_and_country(
    request: Request,
    terms: str = Query(
        description="Specify terms to search for in the variety, title and description"
    ),
    country: str = Query(description="Country name to search for wines from"),
) -> list[SimilaritySearch] | None:
    COLLECTION = "wines"
    result = _search_by_similarity_and_country(request, COLLECTION, terms, country)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided terms '{terms}' found in database - please try again",
        )
    return result


@wine_router.get(
    "/search_by_filters",
    response_model=list[SimilaritySearch],
    response_description="Search for wines via semantically similar terms with added filters",
)
def search_by_similarity_and_filters(
    request: Request,
    terms: str = Query(
        description="Specify terms to search for in the variety, title and description"
    ),
    country: str = Query(description="Country name to search for wines from"),
    points: int = Query(default=85, description="Minimum number of points for a wine"),
    price: float = Query(default=100.0, description="Maximum price for a wine"),
) -> list[SimilaritySearch] | None:
    COLLECTION = "wines"
    result = _search_by_similarity_and_filters(request, COLLECTION, terms, country, points, price)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided terms '{terms}' found in database - please try again",
        )
    return result


@wine_router.get(
    "/count_by_country",
    response_model=CountByCountry,
    response_description="Get counts of wine for a particular country",
)
def count_by_country(
    request: Request,
    country: str = Query(description="Country name to get counts for"),
) -> CountByCountry | None:
    COLLECTION = "wines"
    result = _count_by_country(request, COLLECTION, country)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided country '{country}' found in database - please try again",
        )
    return result


@wine_router.get(
    "/count_by_filters",
    response_model=CountByCountry,
    response_description="Get counts of wine for a particular country, filtered by points and price",
)
def count_by_filters(
    request: Request,
    country: str = Query(description="Country name to get counts for"),
    points: int = Query(default=85, description="Minimum number of points for a wine"),
    price: float = Query(default=100.0, description="Maximum price for a wine"),
) -> CountByCountry | None:
    COLLECTION = "wines"
    result = _count_by_filters(request, COLLECTION, country, points, price)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided country '{country}' found in database - please try again",
        )
    return result


# --- Helper functions ---


def _search_by_similarity(
    request: Request,
    collection: str,
    terms: str,
) -> list[SimilaritySearch] | None:
    if request.app.model_type == "sbert":
        vector = request.app.model.encode(terms, batch_size=64).tolist()
    elif request.app.model_type == "onnx":
        vector = request.app.model(terms, truncate=True)[0][0]

    # Use `vector` for similarity search on the closest vectors in the collection
    search_result = request.app.client.search(
        collection_name=collection, query_vector=vector, top=5
    )
    # `search_result` contains found vector ids with similarity scores along with the stored payload
    # For now we are interested in payload only
    payloads = [hit.payload for hit in search_result]
    if not payloads:
        return None
    return payloads


def _search_by_similarity_and_country(
    request: Request, collection: str, terms: str, country: str
) -> list[SimilaritySearch] | None:
    if request.app.model_type == "sbert":
        vector = request.app.model.encode(terms, batch_size=64).tolist()
    elif request.app.model_type == "onnx":
        vector = request.app.model(terms, truncate=True)[0][0]

    filter = models.Filter(
        **{
            "must": [
                {
                    "key": "country",
                    "match": {
                        "value": country,
                    },
                },
            ]
        }
    )
    search_result = request.app.client.search(
        collection_name=collection, query_vector=vector, query_filter=filter, top=5
    )
    payloads = [hit.payload for hit in search_result]
    if not payloads:
        return None
    return payloads


def _search_by_similarity_and_filters(
    request: Request,
    collection: str,
    terms: str,
    country: str,
    points: int,
    price: float,
) -> list[SimilaritySearch] | None:
    if request.app.model_type == "sbert":
        vector = request.app.model.encode(terms, batch_size=64).tolist()
    elif request.app.model_type == "onnx":
        vector = request.app.model(terms, truncate=True)[0][0]

    filter = models.Filter(
        **{
            "must": [
                {
                    "key": "country",
                    "match": {
                        "value": country,
                    },
                },
                {
                    "key": "price",
                    "range": {
                        "lte": price,
                    },
                },
                {
                    "key": "points",
                    "range": {
                        "gte": points,
                    },
                },
            ]
        }
    )
    search_result = request.app.client.search(
        collection_name=collection, query_vector=vector, query_filter=filter, top=5
    )
    payloads = [hit.payload for hit in search_result]
    if not payloads:
        return None
    return payloads


def _count_by_country(
    request: Request,
    collection: str,
    country: str,
) -> CountByCountry | None:
    filter = models.Filter(
        **{
            "must": [
                {
                    "key": "country",
                    "match": {
                        "value": country,
                    },
                },
            ]
        }
    )
    result = request.app.client.count(collection_name=collection, count_filter=filter)
    if not result:
        return None
    return result


def _count_by_filters(
    request: Request,
    collection: str,
    country: str,
    points: int,
    price: float,
) -> CountByCountry | None:
    filter = models.Filter(
        **{
            "must": [
                {
                    "key": "country",
                    "match": {
                        "value": country,
                    },
                },
                {
                    "key": "price",
                    "range": {
                        "lte": price,
                    },
                },
                {
                    "key": "points",
                    "range": {
                        "gte": points,
                    },
                },
            ]
        }
    )
    result = request.app.client.count(collection_name=collection, count_filter=filter)
    if not result:
        return None
    return result

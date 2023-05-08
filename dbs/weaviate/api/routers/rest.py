from fastapi import APIRouter, HTTPException, Query, Request
from schemas.retriever import CountByCountry, SimilaritySearch

router = APIRouter()


# --- Routes ---


@router.get(
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
    CLASS_NAME = "Wine"
    result = _search_by_similarity(request, CLASS_NAME, terms)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided terms '{terms}' found in database - please try again",
        )
    return result


@router.get(
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
    CLASS_NAME = "Wine"
    result = _search_by_similarity_and_country(request, CLASS_NAME, terms, country)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided terms '{terms}' found in database - please try again",
        )
    return result


@router.get(
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
    CLASS_NAME = "Wine"
    result = _search_by_similarity_and_filters(request, CLASS_NAME, terms, country, points, price)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided terms '{terms}' found in database - please try again",
        )
    return result


@router.get(
    "/count_by_country",
    response_model=CountByCountry,
    response_description="Get counts of wine for a particular country",
)
def count_by_country(
    request: Request,
    country: str = Query(description="Country name to get counts for"),
) -> CountByCountry | None:
    CLASS_NAME = "Wine"
    result = _count_by_country(request, CLASS_NAME, country)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided country '{country}' found in database - please try again",
        )
    return result


@router.get(
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
    CLASS_NAME = "Wine"
    result = _count_by_filters(request, CLASS_NAME, country, points, price)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided country '{country}' found in database - please try again",
        )
    return result


# --- Helper functions ---


def _search_by_similarity(
    request: Request, class_name: str, terms: str
) -> list[SimilaritySearch] | None:
    # Convert input text query into a vector for lookup in the db
    if request.app.model_type == "sbert":
        vector = request.app.model.encode(terms, show_progress_bar=False, batch_size=128).tolist()
    elif request.app.model_type == "onnx":
        vector = request.app.model(terms)[0][0]

    near_vec = {"vector": vector}
    response = (
        request.app.client.query.get(
            class_name,
            [
                "wineID",
                "title",
                "description",
                "country",
                "province",
                "points",
                "price",
                "variety",
                "winery",
                "_additional {certainty}",
            ],
        )
        .with_near_vector(near_vec)
        .with_limit(5)
        .do()
    )
    try:
        payload = response["data"]["Get"][class_name]
        return payload
    except Exception as e:
        print(f"Error {e}: Did not obtain appropriate response from Weaviate")
        return None


def _search_by_similarity_and_country(
    request: Request,
    class_name: str,
    terms: str,
    country: str,
) -> list[SimilaritySearch] | None:
    # Convert input text query into a vector for lookup in the db
    if request.app.model_type == "sbert":
        vector = request.app.model.encode(terms, show_progress_bar=False, batch_size=128).tolist()
    elif request.app.model_type == "onnx":
        vector = request.app.model(terms)[0][0]

    near_vec = {"vector": vector}
    where_filter = {
        "path": "country",
        "operator": "Equal",
        "valueText": country,
    }
    response = (
        request.app.client.query.get(
            class_name,
            [
                "wineID",
                "title",
                "description",
                "country",
                "province",
                "points",
                "price",
                "variety",
                "winery",
                "_additional {certainty}",
            ],
        )
        .with_near_vector(near_vec)
        .with_where(where_filter)
        .with_limit(5)
        .do()
    )
    try:
        payload = response["data"]["Get"][class_name]
        return payload
    except Exception as e:
        print(f"Error {e}: Did not obtain appropriate response from Weaviate")
        return None


def _search_by_similarity_and_filters(
    request: Request,
    class_name: str,
    terms: str,
    country: str,
    points: int,
    price: float,
) -> list[SimilaritySearch] | None:
    # Convert input text query into a vector for lookup in the db
    if request.app.model_type == "sbert":
        vector = request.app.model.encode(terms, show_progress_bar=False, batch_size=128).tolist()
    elif request.app.model_type == "onnx":
        vector = request.app.model(terms)[0][0]

    near_vec = {"vector": vector}
    where_filter = {
        "operator": "And",
        "operands": [
            {
                "path": "country",
                "operator": "Equal",
                "valueText": country,
            },
            {
                "path": "price",
                "operator": "LessThan",
                "valueNumber": price,
            },
            {
                "path": "points",
                "operator": "GreaterThan",
                "valueInt": points,
            },
        ],
    }
    response = (
        request.app.client.query.get(
            class_name,
            [
                "wineID",
                "title",
                "description",
                "country",
                "province",
                "points",
                "price",
                "variety",
                "winery",
                "_additional {certainty}",
            ],
        )
        .with_near_vector(near_vec)
        .with_where(where_filter)
        .with_limit(5)
        .do()
    )
    try:
        payload = response["data"]["Get"][class_name]
        return payload
    except Exception as e:
        print(f"Error {e}: Did not obtain appropriate response from Weaviate")
        return None


def _count_by_country(
    request: Request,
    class_name: str,
    country: str,
) -> CountByCountry | None:
    where_filter = {
        "operator": "And",
        "operands": [
            {
                "path": "country",
                "operator": "Equal",
                "valueText": country,
            }
        ],
    }
    response = (
        request.app.client.query.aggregate(class_name)
        .with_where(where_filter)
        .with_fields("meta {count}")
        .do()
    )
    try:
        payload = response["data"]["Aggregate"][class_name]
        count = payload[0]["meta"]
        return count
    except Exception as e:
        print(f"Error {e}: Did not obtain appropriate response from Weaviate")
        return None


def _count_by_filters(
    request: Request,
    class_name: str,
    country: str,
    points: int,
    price: float,
) -> CountByCountry | None:
    where_filter = {
        "operator": "And",
        "operands": [
            {
                "path": "country",
                "operator": "Equal",
                "valueText": country,
            },
            {
                "path": "price",
                "operator": "LessThan",
                "valueNumber": price,
            },
            {
                "path": "points",
                "operator": "GreaterThan",
                "valueInt": points,
            },
        ],
    }
    response = (
        request.app.client.query.aggregate(class_name)
        .with_where(where_filter)
        .with_fields("meta {count}")
        .do()
    )
    try:
        payload = response["data"]["Aggregate"][class_name]
        count = payload[0]["meta"]
        return count
    except Exception as e:
        print(f"Error {e}: Did not obtain appropriate response from Weaviate")
        return None

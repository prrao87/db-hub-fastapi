from fastapi import APIRouter, HTTPException, Query, Request
from qdrant_client import QdrantClient
from qdrant_client.http import models

from schemas.retriever import SimilaritySearch

wine_router = APIRouter()


# --- Routes ---


@wine_router.get(
    "/search",
    response_model=list[SimilaritySearch],
    response_description="Search wines by title, description and variety",
)
def search_by_similarity(
    request: Request,
    terms: str = Query(description="Search wine by keywords in title, description and variety"),
    max_price: float = Query(
        default=100.0, description="Specify the maximum price for the wine (e.g., 30)"
    ),
    country: str = Query(
        default=None, description="Specify the country of origin for the wine (e.g., Italy)"
    ),
) -> list[SimilaritySearch] | None:
    collection = "wines"
    result = _search_by_similarity(request, collection, terms, max_price, country)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided terms '{terms}' found in database - please try again",
        )
    return result


# --- Helper functions ---


def _search_by_similarity(
    request: Request, collection: str, terms: str, max_price: float, country: str
) -> list[SimilaritySearch] | None:
    """Convert input text query into a vector for lookup in the db"""
    if request.app.model_type == "sbert":
        vector = request.app.model.encode(terms, show_progress_bar=False, batch_size=128).tolist()
    elif request.app.model_type == "onnx":
        vector = request.app.model(terms)[0][0]

    # Define a range filter for wine price
    filter = models.Filter(
        **{
            "must": [
                {
                    "key": "price",
                    "range": {
                        "lte": max_price,
                    },
                },
                {
                    "key": "country",
                    "match": {
                        "value": country,
                    },
                },
            ]
        }
    )

    # Use `vector` for similarity search on the closest vectors in the collection
    search_result = request.app.client.search(
        collection_name=collection, query_vector=vector, query_filter=filter, top=5
    )
    # `search_result` contains found vector ids with similarity scores along with the stored payload
    # For now we are interested in payload only
    payloads = [hit.payload for hit in search_result]
    # # Qdrant doesn't appear to have a sort option for fields other than similarity score, so we just filter it ourselves
    payloads = sorted(payloads, key=lambda x: x["points"], reverse=True)
    if not payloads:
        return None
    return payloads

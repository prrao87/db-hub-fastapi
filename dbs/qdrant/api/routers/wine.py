from qdrant_client import QdrantClient
from qdrant_client.http import models
from fastapi import APIRouter, HTTPException, Query, Request
from sentence_transformers import SentenceTransformer

from schemas.retriever import SimilaritySearch

wine_router = APIRouter()


# --- Routes ---


@wine_router.get(
    "/search",
    response_model=list[SimilaritySearch],
    response_description="Search wines by title, description and variety",
)
def search_by_keywords(
    request: Request,
    terms: str = Query(description="Search wine by keywords in title, description and variety"),
    max_price: float = Query(
        default=10000.0, description="Specify the maximum price for the wine (e.g., 30)"
    ),
) -> list[SimilaritySearch] | None:
    model = request.app.model
    client = request.app.client
    collection = "wines"
    result = _search_by_keywords(client, model, collection, terms, max_price)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided terms '{terms}' found in database - please try again",
        )
    return result


# --- Helper functions ---


def _search_by_keywords(
    client: QdrantClient, model: SentenceTransformer, collection: str, terms: str, max_price: float
) -> list[SimilaritySearch] | None:
    """Convert input text query into a vector for lookup in the db"""
    vector = model.encode(terms).tolist()

    # Define a range filter for wine price
    filter = models.Filter(
        **{
            "must": [
                {
                    "key": "price",
                    "range": {
                        "lte": max_price,
                    },
                }
            ]
        }
    )

    # Use `vector` for similarity search on the closest vectors in the collection
    search_result = client.search(
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

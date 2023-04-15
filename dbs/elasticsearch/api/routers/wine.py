from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, HTTPException, Query, Request
from schemas.retriever import (
    FullTextSearch,
    TopWinesByCountry,
    TopWinesByProvince,
)

wine_router = APIRouter()


# --- Routes ---


@wine_router.get(
    "/search",
    response_model=list[FullTextSearch],
    response_description="Search wines by title, description and variety",
)
async def search_by_keywords(
    request: Request,
    terms: str = Query(
        description="Search wine by keywords in title, description and variety"
    ),
    max_price: int = Query(
        default=10000.0, description="Specify the maximum price for the wine (e.g., 30)"
    ),
) -> list[FullTextSearch] | None:
    result = await _search_by_keywords(request.app.client, terms, max_price)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided terms '{terms}' found in database - please try again",
        )
    return result


@wine_router.get(
    "/top_by_country",
    response_model=list[TopWinesByCountry],
    response_description="Get top-rated wines by country",
)
async def top_by_country(
    request: Request,
    country: str = Query(
        description="Get top-rated wines by country name specified (must be exact name)"
    ),
) -> list[TopWinesByCountry] | None:
    result = await _top_by_country(request.app.client, country)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine from the provided country '{country}' found in database - please enter exact country name",
        )
    return result


@wine_router.get(
    "/top_by_province",
    response_model=list[TopWinesByProvince],
    response_description="Get top-rated wines by province",
)
async def top_by_province(
    request: Request,
    province: str = Query(
        description="Get top-rated wines by province name specified (must be exact name)"
    ),
) -> list[TopWinesByProvince] | None:
    result = await _top_by_province(request.app.client, province)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine from the provided province '{province}' found in database - please enter exact province name",
        )
    return result


# @wine_router.get(
#     "/most_by_variety",
#     response_model=list[MostWinesByVariety],
#     response_description="Get the countries with the most wines above a points-rating of a specified variety (blended or otherwise)",
# )
# async def most_by_variety(
#     request: Request,
#     variety: str = Query(
#         description="Specify the variety of wine to search for (e.g., 'Pinot Noir' or 'Red Blend')"
#     ),
#     points: int = Query(
#         default=85,
#         description="Specify the minimum points-rating for the wine (e.g., 85)",
#     ),
# ) -> list[MostWinesByVariety] | None:
#     result = await _most_by_variety(request.app.client, variety, points)
#     if not result:
#         raise HTTPException(
#             status_code=404,
#             detail=f"No wine of the specified variety '{variety}' found in database - please try a different variety",
#         )
#     return result


# --- Elasticsearch query funcs ---


async def _search_by_keywords(
    client: AsyncElasticsearch, terms: str, max_price: int
) -> list[FullTextSearch] | None:
    response = await client.search(
        index="wines",
        size=5,
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": terms,
                                "fields": ["title", "description", "variety"],
                                "minimum_should_match": 2,
                                "fuzziness": "AUTO",
                            }
                        }
                    ],
                    "filter": {"range": {"price": {"lte": max_price}}},
                }
            },
            "sort": [{"points": {"order": "desc"}}],
        },
    )
    result = response["hits"].get("hits")
    if result:
        data = []
        for item in result:
            data_dict = item["_source"]
            data.append(data_dict)
        return data
    return None


async def _top_by_country(
    client: AsyncElasticsearch, country: str
) -> list[TopWinesByCountry] | None:
    response = await client.search(
        index="wines",
        size=5,
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "country": country,
                            }
                        }
                    ]
                }
            },
            "sort": [{"points": {"order": "desc"}}],
        },
    )
    result = response["hits"].get("hits")
    if result:
        data = []
        for item in result:
            data_dict = item["_source"]
            data.append(data_dict)
        return data
    return None


async def _top_by_province(
    client: AsyncElasticsearch, province: str
) -> list[TopWinesByProvince] | None:
    response = await client.search(
        index="wines",
        size=5,
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "province": province,
                            }
                        }
                    ]
                }
            },
            "sort": [{"points": {"order": "desc"}}],
        },
    )
    result = response["hits"].get("hits")
    if result:
        data = []
        for item in result:
            data_dict = item["_source"]
            data.append(data_dict)
        return data
    return None

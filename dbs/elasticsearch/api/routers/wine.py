from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, HTTPException, Query, Request
from schemas.retriever import (
    CountByCountry,
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
    terms: str = Query(description="Search wine by keywords in title, description and variety"),
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


@wine_router.get(
    "/count_by_country",
    response_model=CountByCountry,
    response_description="Get counts of wine for a particular country",
)
async def count_by_country(
    request: Request,
    country: str = Query(description="Country name to get counts for"),
) -> CountByCountry | None:
    result = await _count_by_country(request.app.client, country)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wines from the provided province '{country}' found in database - please enter exact province name",
        )
    return result


@wine_router.get(
    "/count_by_filters",
    response_model=CountByCountry,
    response_description="Get counts of wine for a particular country, filtered by points and price",
)
async def count_by_filters(
    request: Request,
    country: str = Query(description="Country name to get counts for"),
    points: int = Query(default=85, description="Minimum number of points for a wine"),
    price: float = Query(default=100.0, description="Maximum price for a wine"),
) -> CountByCountry | None:
    result = await _count_by_filters(request.app.client, country, points, price)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wines from the provided province '{country}' found in database - please enter exact province name",
        )
    return result


# --- Elasticsearch query funcs ---


async def _search_by_keywords(
    client: AsyncElasticsearch, terms: str, max_price: int
) -> list[FullTextSearch] | None:
    response = await client.search(
        index="wines",
        size=5,
        query={
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
        sort={"points": {"order": "desc"}},
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
        query={
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
        sort={"points": {"order": "desc"}},
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
        query={
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
        sort={"points": {"order": "desc"}},
    )
    result = response["hits"].get("hits")
    if result:
        data = []
        for item in result:
            data_dict = item["_source"]
            data.append(data_dict)
        return data
    return None


async def _count_by_country(client: AsyncElasticsearch, country: str) -> CountByCountry | None:
    response = await client.count(
        index="wines", query={"bool": {"must": [{"match": {"country": country}}]}}
    )
    result = {"count": response.get("count", 0)}
    return result


async def _count_by_filters(
    client: AsyncElasticsearch, country: str, points: float, price: int
) -> CountByCountry | None:
    response = await client.count(
        index="wines",
        query={
            "bool": {
                "must": [
                    {"match": {"country": country}},
                    {"range": {"points": {"gte": points}}},
                    {"range": {"price": {"lte": price}}},
                ]
            }
        },
    )
    result = {"count": response.get("count", 0)}
    return result

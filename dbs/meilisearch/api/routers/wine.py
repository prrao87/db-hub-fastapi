from meilisearch_python_async import Client
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


# --- Meilisearch query funcs ---


async def _search_by_keywords(
    client: Client, terms: str, max_price: int, index="wines"
) -> list[FullTextSearch] | None:
    index = client.index(index)
    response = await index.search(
        terms,
        limit=5,
        filter=f"price < {max_price}",
        sort=["points:desc" , "price:asc"],
    )
    if response:
        return response.hits
    return None


async def _top_by_country(
    client: Client, country: str, index="wines"
) -> list[TopWinesByCountry] | None:
    index = client.index(index)
    response = await index.search(
        "",
        limit=5,
        filter=f'country = "{country}"',
        sort=["points:desc" , "price:asc"],
    )
    if response:
        return response.hits
    return None


async def _top_by_province(
    client: Client, province: str, index="wines"
) -> list[TopWinesByProvince] | None:
    index = client.index(index)
    response = await index.search(
        "terms",
        limit=5,
        filter=f'province = "{province}"',
        sort=["points:desc" , "price:asc"],
    )
    if response:
        return response.hits
    return None

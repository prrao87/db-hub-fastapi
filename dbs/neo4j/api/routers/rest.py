from fastapi import APIRouter, HTTPException, Query, Request
from neo4j import AsyncManagedTransaction

from api.schemas.rest import (
    FullTextSearch,
    MostWinesByVariety,
    TopWinesByCountry,
    TopWinesByProvince,
)

router = APIRouter()


# --- Routes ---


@router.get(
    "/search",
    response_model=list[FullTextSearch],
    response_description="Search wines by title and description",
)
async def search_by_keywords(
    request: Request,
    terms: str = Query(description="Search wine by keywords in title or description"),
    max_price: float = Query(
        default=100.0, description="Specify the maximum price for the wine (e.g., 30)"
    ),
) -> list[FullTextSearch] | None:
    session = request.app.session
    result = await session.execute_read(_search_by_keywords, terms, max_price)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided terms '{terms}' found in database - please try again",
        )
    return result


@router.get(
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
    session = request.app.session
    result = await session.execute_read(_top_by_country, country)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine from the provided country '{country}' found in database - please enter exact country name",
        )
    return result


@router.get(
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
    session = request.app.session
    result = await session.execute_read(_top_by_province, province)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine from the provided province '{province}' found in database - please enter exact province name",
        )
    return result


@router.get(
    "/most_by_variety",
    response_model=list[MostWinesByVariety],
    response_description="Get the countries with the most wines above a points-rating of a specified variety (blended or otherwise)",
)
async def most_by_variety(
    request: Request,
    variety: str = Query(
        description="Specify the variety of wine to search for (e.g., 'Pinot Noir' or 'Red Blend')"
    ),
    points: int = Query(
        default=85,
        description="Specify the minimum points-rating for the wine (e.g., 85)",
    ),
) -> list[MostWinesByVariety] | None:
    session = request.app.session
    result = await session.execute_read(_most_by_variety, variety, points)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine of the specified variety '{variety}' found in database - please try a different variety",
        )
    return result


# --- Neo4j query funcs ---


async def _search_by_keywords(
    tx: AsyncManagedTransaction,
    terms: str,
    price: float,
) -> list[FullTextSearch] | None:
    query = """
        CALL db.index.fulltext.queryNodes("searchText", $terms) YIELD node AS wine, score
        WITH DISTINCT wine, score
            MATCH (wine)-[:IS_FROM_COUNTRY]->(c:Country)
            WHERE wine.price <= $price
        RETURN
            c.countryName AS country,
            wine.wineID AS wineID,
            wine.points AS points,
            wine.title AS title,
            wine.description AS description,
            coalesce(wine.price, "Not available") AS price,
            wine.variety AS variety,
            wine.winery AS winery
        ORDER BY score DESC, points DESC LIMIT 5
    """
    response = await tx.run(query, terms=terms, price=price)
    result = await response.data()
    if result:
        return [FullTextSearch(**r) for r in result]
    return None


async def _top_by_country(
    tx: AsyncManagedTransaction,
    country: str,
) -> list[TopWinesByCountry] | None:
    query = """
        MATCH (wine:Wine)-[:IS_FROM_COUNTRY]->(c:Country)
        WHERE tolower(c.countryName) = tolower($country)
        RETURN
            wine.wineID AS wineID,
            wine.points AS points,
            wine.title AS title,
            wine.description AS description,
            c.countryName AS country,
            coalesce(wine.price, "Not available") AS price,
            wine.variety AS variety,
            wine.winery AS winery
        ORDER BY points DESC LIMIT 5
    """
    response = await tx.run(query, country=country)
    result = await response.data()
    if result:
        return [TopWinesByCountry(**r) for r in result]
    return None


async def _top_by_province(
    tx: AsyncManagedTransaction,
    province: str,
) -> list[TopWinesByProvince] | None:
    query = """
        MATCH (wine:Wine)-[:IS_FROM_PROVINCE]->(p:Province)-[:IS_LOCATED_IN]->(c:Country)
        WHERE tolower(p.provinceName) = tolower($province)
        RETURN
            wine.wineID AS wineID,
            wine.points AS points,
            wine.title AS title,
            wine.description AS description,
            c.countryName AS country,
            p.provinceName AS province,
            coalesce(wine.price, "Not available") AS price,
            wine.variety AS variety,
            wine.winery AS winery
        ORDER BY points DESC LIMIT 5
    """
    response = await tx.run(query, province=province)
    result = await response.data()
    if result:
        return [TopWinesByProvince(**r) for r in result]
    return None


async def _most_by_variety(
    tx: AsyncManagedTransaction,
    variety: str,
    points: int,
) -> list[MostWinesByVariety] | None:
    query = """
        CALL db.index.fulltext.queryNodes("searchText", $variety) YIELD node AS wine, score
        WITH wine
            MATCH (wine)-[:IS_FROM_COUNTRY]->(c:Country)
            WHERE wine.points >= $points
        RETURN
            c.countryName AS country,
            count(wine) as wineCount
        ORDER BY wineCount DESC LIMIT 5
    """
    response = await tx.run(query, variety=variety, points=points)
    result = await response.data()
    if result:
        return [MostWinesByVariety(**r) for r in result]
    return None

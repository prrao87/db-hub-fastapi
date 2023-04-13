from fastapi import APIRouter, HTTPException, Query, Request
from neo4j import AsyncManagedTransaction

from schemas.wine import SearchByKeywords

wine_router = APIRouter()


# --- Routes ---


@wine_router.get(
    "/search",
    response_model=list[SearchByKeywords],
    response_description="Search wines by title and description",
)
async def create_constraint(
    request: Request,
    search_string: str = Query(
        description="Search wine by keywords in title or description"
    ),
) -> list[SearchByKeywords] | None:
    session = request.app.state.db_session
    result = await session.execute_read(_search_by_title_and_desc, search_string)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided terms '{search_string}' found in database - please try again",
        )
    return result


# --- Neo4j query funcs ---


async def _search_by_title_and_desc(
    tx: AsyncManagedTransaction,
    search_string: str,
) -> list[SearchByKeywords] | None:
    # Convert search string to a list of terms separated by AND
    search_params = " AND ".join(search_string.split())
    query = """
        CALL db.index.fulltext.queryNodes("titlesAndDescriptions", $params) YIELD node, score
        WITH node, score
            MATCH (node)-[:IS_FROM_COUNTRY]->(c:Country)
        RETURN
            c.countryName as country,
            node.wineID as wineID,
            node.title as title,
            node.points as points,
            coalesce(node.price, "Not available") as price,
            node.variety as variety,
            node.winery as winery
        ORDER BY score DESC, points DESC LIMIT 5
    """
    result = await tx.run(query, params=search_params)
    record = await result.data()
    return record

from fastapi import APIRouter, HTTPException, Query, Request

from api.schemas.rest import CountByCountry, SimilaritySearch

router = APIRouter()

NUM_PROBES = 20

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
    result = _search_by_similarity(request, terms)
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
    result = _search_by_similarity_and_country(request, terms, country)
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
    result = _search_by_similarity_and_filters(request, terms, country, points, price)
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
) -> CountByCountry:
    result = _count_by_country(request, country)
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
) -> CountByCountry:
    result = _count_by_filters(request, country, points, price)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No wine with the provided country '{country}' found in database - please try again",
        )
    return result


# --- Helper functions ---


def _search_by_similarity(
    request: Request,
    terms: str,
) -> list[SimilaritySearch] | None:
    query_vector = request.app.model.encode(terms.lower())
    search_result = (
        request.app.table.search(query_vector).metric("cosine").nprobes(NUM_PROBES).limit(5).to_df()
    ).to_dict(orient="records")
    if not search_result:
        return None
    return search_result


def _search_by_similarity_and_country(
    request: Request, terms: str, country: str
) -> list[SimilaritySearch] | None:
    query_vector = request.app.model.encode(terms.lower())
    search_result = (
        request.app.table.search(query_vector)
        .metric("cosine")
        .nprobes(NUM_PROBES)
        .where(
            f"""
            country = '{country}'
            """
        )
        .limit(5)
        .to_df()
    ).to_dict(orient="records")
    if not search_result:
        return None
    return search_result


def _search_by_similarity_and_filters(
    request: Request,
    terms: str,
    country: str,
    points: int,
    price: float,
) -> list[SimilaritySearch] | None:
    query_vector = request.app.model.encode(terms.lower())
    price = float(price)
    search_result = (
        request.app.table.search(query_vector)
        .metric("cosine")
        .nprobes(NUM_PROBES)
        .where(
            f"""
            country = '{country}'
            and points >= {points}
            and price <= {price}
            """
        )
        .limit(5)
        .to_df()
    ).to_dict(orient="records")
    if not search_result:
        return None
    return search_result


def _count_by_country(
    request: Request,
    country: str,
) -> CountByCountry:
    search_result = (
        request.app.table.search()
        .where(
            f"""
            country = '{country}'
            """
        )
        .to_df()
    ).shape[0]
    final_result = CountByCountry(count=search_result)
    return final_result


def _count_by_filters(
    request: Request,
    country: str,
    points: int,
    price: float,
) -> CountByCountry:
    price = float(price)
    search_result = (
        request.app.table.search()
        .where(
            f"""
            country = '{country}'
            and points >= {points}
            and price <= {price}
            """
        )
        .to_df()
    ).shape[0]
    final_result = CountByCountry(count=search_result)
    return final_result

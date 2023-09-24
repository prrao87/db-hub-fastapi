from typing import Optional
from pydantic import BaseModel, ConfigDict


class SimilaritySearch(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={
            "example": {
                "wineID": 3845,
                "country": "Italy",
                "title": "Castellinuzza e Piuca 2010  Chianti Classico",
                "description": "This gorgeous Chianti Classico boasts lively cherry, strawberry and violet aromas. The mouthwatering palate shows concentrated wild-cherry flavor layered with mint, white pepper and clove. It has fresh acidity and firm tannins that will develop complexity with more bottle age. A textbook Chianti Classico.",
                "points": 93,
                "price": 16,
                "variety": "Red Blend",
                "winery": "Castellinuzza e Piuca",
            }
        },
    )

    id: int
    country: str
    province: Optional[str]
    title: str
    description: Optional[str]
    points: int
    price: Optional[float]
    variety: Optional[str]
    winery: Optional[str]


class CountByCountry(BaseModel):
    count: int
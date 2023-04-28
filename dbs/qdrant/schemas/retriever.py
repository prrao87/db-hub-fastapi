from pydantic import BaseModel


class SimilaritySearch(BaseModel):
    id: int
    country: str
    province: str | None
    title: str
    description: str | None
    points: int
    price: float | str | None
    variety: str | None
    winery: str | None

    class Config:
        extra = "ignore"
        schema_extra = {
            "example": {
                "id": 3845,
                "country": "Italy",
                "title": "Castellinuzza e Piuca 2010  Chianti Classico",
                "description": "This gorgeous Chianti Classico boasts lively cherry, strawberry and violet aromas. The mouthwatering palate shows concentrated wild-cherry flavor layered with mint, white pepper and clove. It has fresh acidity and firm tannins that will develop complexity with more bottle age. A textbook Chianti Classico.",
                "points": 93,
                "price": 16,
                "variety": "Red Blend",
                "winery": "Castellinuzza e Piuca",
            }
        }


class CountByCountry(BaseModel):
    count: int | None

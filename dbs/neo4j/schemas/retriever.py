from pydantic import BaseModel, ConfigDict


class FullTextSearch(BaseModel):
    model_config = ConfigDict(
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
        }
    )

    wineID: int
    country: str
    title: str
    description: str | None
    points: int
    price: float | str
    variety: str | None
    winery: str | None

class TopWinesByCountry(BaseModel):
    wineID: int
    country: str
    title: str
    description: str | None
    points: int
    price: float | str
    variety: str | None
    winery: str | None


class TopWinesByProvince(BaseModel):
    wineID: int
    country: str
    province: str
    title: str
    description: str | None
    points: int
    price: float | str
    variety: str | None
    winery: str | None


class MostWinesByVariety(BaseModel):
    country: str
    wineCount: int

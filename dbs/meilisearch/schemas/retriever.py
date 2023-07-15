from pydantic import BaseModel, ConfigDict


class FullTextSearch(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
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
    )

    id: int
    country: str
    title: str
    description: str | None
    points: int
    price: float | str | None
    variety: str | None
    winery: str | None


class TopWinesByCountry(BaseModel):
    model_config = ConfigDict(
        validate_assignment=True,
    )

    id: int
    country: str
    title: str
    description: str | None
    points: int
    price: float | str | None = "Not available"
    variety: str | None
    winery: str | None


class TopWinesByProvince(BaseModel):
    model_config = ConfigDict(
        validate_assignment=True,
    )

    id: int
    country: str
    province: str
    title: str
    description: str | None
    points: int
    price: float | str | None = "Not available"
    variety: str | None
    winery: str | None

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Wine(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        extra="allow",
        str_strip_whitespace=True,
        json_schema_extra = {
            "example": {
                "id": 45100,
                "points": 85,
                "title": "Balduzzi 2012 Reserva Merlot (Maule Valley)",
                "description": "Ripe in color and aromas, this chunky wine delivers heavy baked-berry and raisin aromas in front of a jammy, extracted palate. Raisin and cooked berry flavors finish plump, with earthy notes.",
                "price": 10.0,
                "variety": "Merlot",
                "winery": "Balduzzi",
                "vineyard": "Reserva",
                "country": "Chile",
                "province": "Maule Valley",
                "region_1": "null",
                "region_2": "null",
                "taster_name": "Michael Schachner",
                "taster_twitter_handle": "@wineschach",
            }
        }
    )

    id: int
    points: int
    title: str
    description: str | None
    price: float | None
    variety: str | None
    winery: str | None
    vineyard: str | None = Field(..., alias="designation")
    country: str | None
    province: str | None
    region_1: str | None
    region_2: str | None
    taster_name: str | None
    taster_twitter_handle: str | None

    @model_validator(mode="before")
    def _fill_country_unknowns(cls, values):
        "Fill in missing country values with 'Unknown', as we always want this field to be queryable"
        country = values.get("country")
        if country is None or country == "null":
            values["country"] = "Unknown"
        return values


if __name__ == "__main__":
    data = {
        "id": 45100,
        "points": 85,
        "title": "Balduzzi 2012 Reserva Merlot (Maule Valley)",
        "description": "Ripe in color and aromas, this chunky wine delivers heavy baked-berry and raisin aromas in front of a jammy, extracted palate. Raisin and cooked berry flavors finish plump, with earthy notes.",
        "price": 10,   # Test if field is cast to float
        "variety": "Merlot",
        "winery": "Balduzzi",
        "designation": "Reserva",   # Test if field is renamed
        "country": "null",   # Test unknown country
        "province": " Maule Valley ",   # Test if field is stripped
        "region_1": "null",
        "region_2": "null",
        "taster_name": "Michael Schachner",
        "taster_twitter_handle": "@wineschach",
    }
    from pprint import pprint
    wine = Wine(**data)
    pprint(wine.model_dump())
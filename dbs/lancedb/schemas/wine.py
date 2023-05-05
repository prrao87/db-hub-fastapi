from pydantic import BaseModel, root_validator


class Wine(BaseModel):
    id: int
    points: int
    title: str
    description: str | None
    price: float | None
    variety: str | None
    winery: str | None
    vineyard: str | None
    country: str | None
    province: str | None
    region_1: str | None
    region_2: str | None
    taster_name: str | None
    taster_twitter_handle: str | None

    class Config:
        extra = "allow"
        allow_population_by_field_name = True
        validate_assignment = True
        schema_extra = {
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

    @root_validator(pre=True)
    def _get_vineyard(cls, values):
        "Rename designation to vineyard"
        vineyard = values.pop("designation", None)
        if vineyard:
            values["vineyard"] = vineyard.strip()
        return values

    @root_validator
    def _fill_country_unknowns(cls, values):
        "Fill in missing country values with 'Unknown', as we always want this field to be queryable"
        country = values.get("country")
        if not country:
            values["country"] = "Unknown"
        return values

    @root_validator
    def _add_to_vectorize_fields(cls, values):
        "Add a field to_vectorize that will be used to create sentence embeddings"
        variety = values.get("variety", "")
        title = values.get("title", "")
        description = values.get("description", "")
        to_vectorize = list(filter(None, [variety, title, description]))
        values["to_vectorize"] = " ".join(to_vectorize).strip()
        return values

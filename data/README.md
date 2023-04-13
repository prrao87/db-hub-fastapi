# Dataset: 130k wine reviews

We use this [wine reviews dataset from Kaggle](https://www.kaggle.com/datasets/zynicide/wine-reviews) as input. The data consists of 130k wine reviews with the variety, location, winery, price, description, and some other metadata provided for each wine. Refer to the Kaggle source for more information on the data.

For quick reference, a sample wine item in JSON format is shown below.

```json
{
    "points": "90",
    "title": "Castello San Donato in Perano 2009 Riserva  (Chianti Classico)",
    "description": "Made from a blend of 85% Sangiovese and 15% Merlot, this ripe wine delivers soft plum, black currants, clove and cracked pepper sensations accented with coffee and espresso notes. A backbone of firm tannins give structure. Drink now through 2019.",
    "taster_name": "Kerin O'Keefe",
    "taster_twitter_handle": "@kerinokeefe",
    "price": 30,
    "designation": "Riserva",
    "variety": "Red Blend",
    "region_1": "Chianti Classico",
    "region_2": null,
    "province": "Tuscany",
    "country": "Italy",
    "winery": "Castello San Donato in Perano",
    "id": 40825
}

```

The data is converted to a ZIP achive, and the code for this as well as the ZIP data is provided here for reference. There is no need to rerun the code to reproduce the results in the rest of the code base in this repo.

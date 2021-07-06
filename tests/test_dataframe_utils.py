from datetime import date

import pandas as pd

from utils.dataframe_utils import (
    find_max_temp_city,
    find_max_temp_delta_city,
    find_max_temp_diff,
    find_min_temp_city,
)

observation_dates = [
    date(2021, 8, 29),
    date(2021, 8, 30),
    date(2021, 8, 31),
    date(2021, 9, 1),
    date(2021, 9, 2),
]

kuopio_weather = pd.DataFrame(
    {
        "date": observation_dates,
        "max_temp": [18, 19, 18, 22, 23],
        "min_temp": [12, 15, 14, 18, 19],
    }
)

suojarvi_weather = pd.DataFrame(
    {
        "date": observation_dates,
        "max_temp": [18, 22, 25, 24, 28],
        "min_temp": [14, 15, 17, 17, 16],
    }
)

city_weather_dict = {
    ("FI", "Kuopio"): kuopio_weather,
    ("RU", "Suojarvi"): suojarvi_weather,
}


def test_find_max_temperature():
    expected_res = pd.DataFrame(
        {"date": date(2021, 9, 2), "temp": 28, "Country": "RU", "City": "Suojarvi"},
        index=[4],
    )
    actual_res = find_max_temp_city(city_weather_dict)
    pd.testing.assert_frame_equal(expected_res, actual_res, check_like=True)


def test_find_min_temperature():
    expected_res = pd.DataFrame(
        {"date": date(2021, 8, 29), "temp": 12, "Country": "FI", "City": "Kuopio"},
        index=[0],
    )
    actual_res = find_min_temp_city(city_weather_dict)
    pd.testing.assert_frame_equal(expected_res, actual_res, check_like=True)


def test_find_max_daily_difference():
    expected_res = pd.DataFrame(
        {
            "date": date(2021, 9, 2),
            "temp_diff": 12,
            "Country": "RU",
            "City": "Suojarvi",
        },
        index=[4],
    )
    actual_res = find_max_temp_diff(city_weather_dict)
    pd.testing.assert_frame_equal(
        expected_res, actual_res, check_like=True, check_dtype=False
    )


def test_find_max_change_over_time():
    expected_res = pd.DataFrame(
        {"temp_delta": [6, 6], "Country": ["FI", "RU"], "City": ["Kuopio", "Suojarvi"]}
    )
    actual_res = find_max_temp_delta_city(city_weather_dict)
    pd.testing.assert_frame_equal(
        expected_res, actual_res, check_like=True, check_dtype=False
    )

import json
from datetime import date, datetime

import pandas as pd

from utils.async_utils import parse_forecasted_data, parse_historic_data


def test_parse_forecast():
    with open("tests/test_data/forecast.json") as json_file:
        forecast = json.loads(json_file.read())

    actual_res = parse_forecasted_data(forecast)

    expected_res = pd.DataFrame(
        {
            "date": [
                date(2021, 7, 4),
                date(2021, 7, 5),
                date(2021, 7, 6),
                date(2021, 7, 7),
                date(2021, 7, 8),
                date(2021, 7, 9),
                date(2021, 7, 10),
                date(2021, 7, 11),
            ],
            "max_temp": [28.62, 27.67, 33.81, 34.53, 35.23, 37.16, 21.16, 17.73],
            "min_temp": [16.35, 17.47, 17.88, 19.99, 20.11, 20.09, 17.73, 16.27],
        }
    )

    pd.testing.assert_frame_equal(expected_res, actual_res, check_like=True)


def test_history_data_parsing():
    with open("tests/test_data/history.json") as json_file:
        history = json.loads(json_file.read())

    actual_res = parse_historic_data(history)

    expected_res = pd.Series(
        {
            "date": date(2021, 7, 4),
            "max_temp": 27.29,
            "min_temp": 16.12,
        },
        name=(date(2021, 7, 4) - datetime.utcnow().date()).days,
    )

    pd.testing.assert_series_equal(expected_res, actual_res, check_category_order=False)

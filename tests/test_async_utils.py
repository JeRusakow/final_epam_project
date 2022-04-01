import json
import random
import aiohttp
import pytest
from asyncmock import AsyncMock
from datetime import date, datetime

import pandas as pd

from utils.async_utils import (
    get_adress_by_coordinates,
    get_addresses,
    get_weather,
    get_weather_bulk,
    parse_forecasted_data,
    parse_historic_data,
    date_range
)


@pytest.mark.asyncio
async def test_get_address_by_coordinates():
    mock_response = AsyncMock()
    mock_response.address = "address"
    mock_rev_geoloc = AsyncMock(return_value=mock_response)

    res = await get_adress_by_coordinates(1.05, 55.32, mock_rev_geoloc)

    assert res == "address"


@pytest.mark.asyncio
async def test_get_addresses(mocker):
    test_size = 10
    coords = [(random.random(), random.random()) for _ in range(test_size)]

    mock_get_address = mocker.patch("utils.async_utils.get_adress_by_coordinates", return_value="Address")
    res = await get_addresses(coords, 10)

    assert res == ["Address"] * test_size


@pytest.mark.asyncio
async def test_get_weather(mocker):
    with open("tests/test_data/forecast.json") as json_file:
        test_curr_forecast_data = json.loads(json_file.read())

    with open("tests/test_data/history.json") as json_file:
        test_historical_data = json.loads(json_file.read())

    test_history_depth = 4
    side_effects = [test_curr_forecast_data] + [test_historical_data] * test_history_depth
    mock_make_request = mocker.patch("utils.async_utils.make_request", side_effect=side_effects)

    async with aiohttp.ClientSession() as test_session:
        res = await get_weather(2.22, 2.55, test_session, history_depth=test_history_depth)

    test_historical_day = pd.Series(
        {
            "date": date(2021, 7, 4),
            "max_temp": 27.29,
            "min_temp": 16.12,
        },
        name=(date(2021, 7, 4) - datetime.utcnow().date()).days,
    )
    expected_history_data = pd.DataFrame([test_historical_day] * test_history_depth)

    expected_curr_and_future_data = pd.DataFrame(
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
    expected_res = pd.concat([expected_history_data, expected_curr_and_future_data])

    pd.testing.assert_frame_equal(expected_res, res, check_like=True)


@pytest.mark.asyncio
async def test_get_weather_bulk(mocker):
    test_length = 10
    test_history_depth = 4
    mock_get_weather = mocker.patch("utils.async_utils.get_weather", return_value="Weather")
    test_coords = [(random.random(), random.random()) for _ in range(test_length)]

    res = await get_weather_bulk(test_coords, history_depth=test_history_depth)
    assert res == ["Weather"] * 10


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


def test_date_range():
    res = [date for date in date_range(date(2019, 1, 1), date(2019, 1, 5))]
    expected_res = [
        date(2019, 1, 1),
        date(2019, 1, 2),
        date(2019, 1, 3),
        date(2019, 1, 4)
    ]

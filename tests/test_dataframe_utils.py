from datetime import date

import pandas as pd

from utils.dataframe_utils import (
    refine_data,
    select_most_hoteled_cities,
    find_max_temp_city,
    find_max_temp_delta_city,
    find_max_temp_diff,
    find_min_temp_city,
)


# Test data for testing refine_data method
hotels_data_raw = pd.DataFrame(
    {
        "Id": [35546, 555, 655, 3, 56, 8],
        "Name": ["Some name", "Some name", "Some name", "Some name", "Some name", "Some name"],
        "Country": ["FI", "EN", "US", "PL", "US", None],
        "City": ["Helsinki", "London", "Washington", "Warsaw", "Boston", "Skyrim"],
        "Latitude": ["33.55454", "228.55", "66.5", "-16.5", "-8kk9", "55.44"],
        "Longitude": ["55.55", "68.644", "-44.5", "-199.5", "13.58", "33.5"]
    },
    index=[0, 1, 2, 3, 4, 5]
)

hotels_data_refined = pd.DataFrame(
    {
        "Name": ["Some name", "Some name"],
        "Country": ["FI", "US"],
        "City": ["Helsinki", "Washington"],
        "Latitude": [33.55454, 66.5],
        "Longitude": [55.55, -44.5]
    },
    index=[0, 2]
)

# Test data for test_find_most_hoteled_cities

hotels_data_for_aggregation_for_single_res = pd.DataFrame(
    {
        "Name": ["Name1", "Name2", "Name3"],
        "Country": ["FI", "FI", "FI"],
        "City": ["City1", "City1", "City2"],
        "Latitude": [33.55454, 66.5, 1.22],
        "Longitude": [55.55, -44.5, -5.55]
    }
)

hotels_data_for_aggregation_for_single_res_exp = pd.DataFrame(
    {
        "Country": ["FI"],
        "City": ["City1"]
    }
)

hotels_data_for_aggregation_for_multiple_res = pd.DataFrame(
    {
        "Name": ["Name1", "Name2", "Name3", "Name4", "Name5"],
        "Country": ["FI", "FI", "FI", "FI", "FI"],
        "City": ["City1", "City1", "City2", "City2", "City3"],
        "Latitude": [33.55454, 66.5, 1.22, 22.2, 3.89],
        "Longitude": [55.55, -44.5, -5.55, -9.66, -84.55]
    }
)

hotels_data_for_aggregation_for_multiple_res_exp = pd.DataFrame(
    {
        "Country": ["FI", "FI"],
        "City": ["City1", "City2"]
    }
)


# Data for testing find_XXX_temperature

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
        "max_temp": [0, 0, 0, 100, 50],
        "min_temp": [0, 0, 0, -100, 0]
    }
)

sekke_weather = pd.DataFrame(
    {
        "date": observation_dates,
        "max_temp": [0, 100, 50, 10, 50],
        "min_temp": [0, -100, 0, 0, 0]
    }
)

kostamus_weather = pd.DataFrame(
    {
        "date": observation_dates,
        "max_temp": [11, 12, 15, 14, 10],
        "min_temp": [10, 7, 9, 8, 10]
    }
)

city_weather_dict_for_single_result = {
        ("FI", "Kuopio"): kuopio_weather,
        ("RU", "Kostamus"): kostamus_weather
    }

city_weather_dict_for_multiple_result = {
    ("FI", "Kuopio"): kuopio_weather,
    ("RU", "Sekke"): sekke_weather,
    ("RU", "Kostamus"): kostamus_weather
}

#
# Testing section
#


def test_refine_data():
    actual_res = refine_data(hotels_data_raw)
    pd.testing.assert_frame_equal(hotels_data_refined, actual_res, check_index_type=False)


def test_select_most_hoteled_cities_for_single_country():
    actual_res = select_most_hoteled_cities(hotels_data_for_aggregation_for_single_res)
    pd.testing.assert_frame_equal(hotels_data_for_aggregation_for_single_res_exp, actual_res, check_index_type=False)


def test_select_most_hoteled_cities_for_multiple_counties():
    actual_res = select_most_hoteled_cities(hotels_data_for_aggregation_for_multiple_res)
    pd.testing.assert_frame_equal(hotels_data_for_aggregation_for_multiple_res_exp, actual_res, check_index_type=False)


# Testing find_XXX_temperature methods

def test_find_max_temperature_single_result():
    expected_res = pd.DataFrame(
        {
            "date": [observation_dates[3]],
            "temp": [100],
            "Country": ["FI"],
            "City": ["Kuopio"]
        },
        index=[3],
    )
    actual_res = find_max_temp_city(city_weather_dict_for_single_result)
    pd.testing.assert_frame_equal(expected_res, actual_res, check_like=True)


def test_find_max_temperature_multiple_result():
    expected_res = pd.DataFrame(
        {
            "date": [observation_dates[3], observation_dates[1]],
            "temp": [100, 100],
            "Country": ["FI", "RU"],
            "City": ["Kuopio", "Sekke"]
        },
        index=[3, 1]
    )
    actual_res = find_max_temp_city(city_weather_dict_for_multiple_result)
    pd.testing.assert_frame_equal(expected_res, actual_res, check_like=True)


def test_find_min_temperature_single_result():
    expected_res = pd.DataFrame(
        {
            "date": [observation_dates[3]],
            "temp": [-100],
            "Country": ["FI"],
            "City": ["Kuopio"]
        },
        index=[3],
    )
    actual_res = find_min_temp_city(city_weather_dict_for_single_result)
    pd.testing.assert_frame_equal(expected_res, actual_res, check_like=True)


def test_find_min_temperature_multiple_result():
    expected_res = pd.DataFrame(
        {
            "date": [observation_dates[3], observation_dates[1]],
            "temp": [-100, -100],
            "Country": ["FI", "RU"],
            "City": ["Kuopio", "Sekke"]
        },
        index=[3, 1]
    )
    actual_res = find_min_temp_city(city_weather_dict_for_multiple_result)
    pd.testing.assert_frame_equal(expected_res, actual_res, check_like=True)


def test_find_max_daily_difference_single_result():
    expected_res = pd.DataFrame(
        {
            "date": [observation_dates[3]],
            "temp_diff": 200,
            "Country": "FI",
            "City": "Kuopio",
        },
        index=[3],
    )
    actual_res = find_max_temp_diff(city_weather_dict_for_single_result)
    pd.testing.assert_frame_equal(
        expected_res, actual_res, check_like=True, check_dtype=False
    )


def test_find_max_daily_difference_multiple_result():
      expected_res = pd.DataFrame(
          {
              "date": [observation_dates[3], observation_dates[1]],
              "temp_diff": [200, 200],
              "Country": ["FI", "RU"],
              "City": ["Kuopio", "Sekke"]
          },
          index=[3, 1],
      )
      actual_res = find_max_temp_diff(city_weather_dict_for_multiple_result)
      pd.testing.assert_frame_equal(
          expected_res, actual_res, check_like=True, check_dtype=False
      )


def test_find_max_change_over_time_single_result():
    expected_res = pd.DataFrame(
        {
            "temp_delta": [25],
            "Country": ["FI"],
            "City": ["Kuopio"]
        }
    )
    actual_res = find_max_temp_delta_city(city_weather_dict_for_single_result)
    pd.testing.assert_frame_equal(
        expected_res, actual_res, check_like=True, check_dtype=False
    )


def test_find_max_change_over_time_multiple_result():
    expected_res = pd.DataFrame(
        {
            "temp_delta": [25, 25],
            "Country": ["FI", "RU"],
            "City": ["Kuopio", "Sekke"]
        },
    )
    actual_res = find_max_temp_delta_city(city_weather_dict_for_multiple_result)
    pd.testing.assert_frame_equal(expected_res, actual_res, check_like=True, check_dtype=False)

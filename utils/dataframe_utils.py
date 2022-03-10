"""This module contains functions providing dataframe processing"""

from os import PathLike

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


def refine_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Purge rows with invalid data from dataframe. Invalid data includes NaNs, Nones,
        latitude out of range (-90;90) degrees, longitude out of range (-180;180)
        degrees.

    Args:
        dataframe: a dataframe to be cleaned

    Returns:
        Cleaned dataframe.
    """
    # Drops ID column because is seems unnecessary
    dataframe.drop(columns=["Id"], inplace=True)

    # Convert some values to float
    dataframe[["Latitude", "Longitude"]] = dataframe[["Latitude", "Longitude"]].apply(
        pd.to_numeric, axis=1, errors="coerce"
    )

    # Drops rows with NaN values
    dataframe.dropna(axis="index", inplace=True)

    # Selects rows with latitude in (-90,90) and longitude in (-180,180)
    dataframe = dataframe[
        (dataframe["Latitude"] <= 90.0)
        & (dataframe["Latitude"] >= -90.0)
        & (dataframe["Longitude"] <= 180.0)
        & (dataframe["Longitude"] >= -180.0)
    ]

    return dataframe


def select_most_hoteled_cities(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Selects cities with most hotels across each Country from dataframe.

    Args:
        dataframe: A DataFrame with information about hotels. Must have columns
            "Country", "City" for hotel location and ome more for hotel info.

    Returns:
    DataFrame with "Country", "City" and "size" column for an amount of hotels per each
        country.
    """
    countries = np.unique(dataframe["Country"])

    hotels_grouped = dataframe.groupby(
        ["Country", "City"], group_keys=False, as_index=False
    ).size()
    most_hotels_df_list = []

    for country in countries:
        hotels_in_country = (
            hotels_grouped[hotels_grouped["Country"] == country]
            .sort_values(["size"], ascending=False, inplace=False)
            .head(1)[["Country", "City"]]
        )

        most_hotels_df_list.append(hotels_in_country)

    return pd.concat(most_hotels_df_list)


def draw_and_save_temp_graph(
    temp_data: pd.DataFrame, save_path: PathLike, city_name=None, today=None
):
    """
    Generates and saves min and max temperatures over dates plot from DataFrame.

    Args:
        temp_data: DataFrame with temperature data. Must have three columns:
            "min_temp", "max_temp", "date".
        save_path: path to save the temperature plot
        city_name (str): provided is included into plot title and filename
        today (date): the current date, gets highlighted on a plot

    Returns:
    None
    """
    fig, axis = plt.subplots()
    axis.plot(
        temp_data[["date"]], temp_data[["min_temp"]], c="cyan", label="Min temperature"
    )
    axis.plot(
        temp_data[["date"]], temp_data[["max_temp"]], c="red", label="Max temperature"
    )
    axis.set_xlabel("Date")
    axis.set_ylabel("Temperature C")
    axis.set_axisbelow(True)
    axis.grid(True)
    axis.margins(x=0)
    axis.legend()

    fig.autofmt_xdate(rotation=45)

    if city_name is not None:
        axis.set_title(f"Weather in {city_name}")

    if today is not None:
        axis.axvline(x=today, c="black")

    fig.savefig(fname=f"{save_path}/weather_{city_name.lower()}.png")


def find_max_temp_city(weather_dict: dict) -> pd.DataFrame:
    """
    Finds city and date where maximal temperature was registered.

    Args:
        weather_dict: A dictionary of {(country, city): weather_in_city_df}. Where
            weather_in_city_df contains date, max_temp, min_temp for a city. This
            data is supposed to be gotten from async_utils.get_weather

    Returns:
    A DataFrame with one or (extremely rarely) more rows of max temperature data.
        This DF contains columns "date", "temp", "Country" and "City".
    """
    max_temps = []
    for (country, city), weather_df in weather_dict.items():
        max_temp = weather_df[
            weather_df["max_temp"] == weather_df["max_temp"].max()
        ].copy()
        max_temp["Country"] = [country] * len(max_temp)
        max_temp["City"] = [city] * len(max_temp)
        max_temp.rename(columns={"max_temp": "temp"}, inplace=True)
        max_temp.drop(columns=["min_temp"], inplace=True)
        max_temps.append(max_temp)

    max_temps = pd.concat(max_temps)
    return max_temps[max_temps["temp"] == max_temps["temp"].max()]


def find_min_temp_city(weather_dict: dict) -> pd.DataFrame:
    """
    Finds city and date where minimal temperature was registered over given period
    of time.

    Args:
        weather_dict: A dictionary of {(country, city): weather_in_city_df}. Where
            weather_in_city_df contains date, max_temp, min_temp for a city. This
            data is supposed to be gotten from async_utils.get_weather

    Returns:
    A DataFrame with one or (extremely rarely) more rows of min temperature data.
        This DF contains columns "date", "temp", "Country" and "City".
    """
    min_temps = []
    for (country, city), weather_df in weather_dict.items():
        min_temp = weather_df[
            weather_df["min_temp"] == weather_df["min_temp"].min()
        ].copy()
        min_temp["Country"] = [country] * len(min_temp)
        min_temp["City"] = [city] * len(min_temp)
        min_temp.rename(columns={"min_temp": "temp"}, inplace=True)
        min_temp.drop(columns=["max_temp"], inplace=True)
        min_temps.append(min_temp)

    max_temps = pd.concat(min_temps)
    return max_temps[max_temps["temp"] == max_temps["temp"].min()]


def find_max_temp_diff(weather_dict: dict) -> pd.DataFrame:
    """
    Finds city and date when maximal difference between max and min temperatures is
    registered.

    Args:
        weather_dict: A dictionary of {(country, city): weather_in_city_df}. Where
            weather_in_city_df contains date, max_temp, min_temp for a city. This
            data is supposed to be gotten from async_utils.get_weather

    Returns:
    A DataFrame with one or (extremely rarely) more rows of temperature change data.
        This DF contains columns "date", "temp_diff", "Country" and "City".
    """
    temp_diffs = []
    for (country, city), weather_df in weather_dict.items():
        temp_diff = pd.DataFrame(
            {
                "date": weather_df["date"],
                "temp_diff": weather_df["max_temp"] - weather_df["min_temp"],
                "Country": [country] * len(weather_df),
                "City": [city] * len(weather_df),
            }
        )
        temp_diffs.append(
            temp_diff[temp_diff["temp_diff"] == temp_diff["temp_diff"].max()]
        )
    temp_diffs = pd.concat(temp_diffs)
    return temp_diffs[temp_diffs["temp_diff"] == temp_diffs["temp_diff"].max()]


def find_max_temp_delta_city(weather_dict: dict) -> pd.DataFrame:
    """
    Finds the city undergone the largest temperature positive change throughout all
    the data. Compared are mean temperatures of max_temp and min_temp.

    Args:
        weather_dict: A dictionary of {(country, city): weather_in_city_df}. Where
            weather_in_city_df contains date, max_temp, min_temp for a city. This
            data is supposed to be gotten from async_utils.get_weather

    Returns:
    A DataFrame with one or (extremely rarely) more rows of temperature data.
        This DF contains columns "temp_delta", "Country" and "City".
    """
    temp_deltas = []
    for (country, city), weather_df in weather_dict.items():
        mean_temps = weather_df[["max_temp", "min_temp"]].mean(axis=1)
        temp_deltas.append(
            pd.DataFrame(
                {
                    "Country": country,
                    "City": city,
                    "temp_delta": mean_temps.tail(1).values - mean_temps.head(1).values,
                },
                index=[None],
            )
        )
    temp_deltas = pd.concat(temp_deltas)
    return temp_deltas[
        temp_deltas["temp_delta"] == temp_deltas["temp_delta"].max()
    ].reset_index(drop=True)

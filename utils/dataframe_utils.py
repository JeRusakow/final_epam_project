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
    fig, ax = plt.subplots()
    ax.plot(
        temp_data[["date"]], temp_data[["min_temp"]], c="cyan", label="Min temperature"
    )
    ax.plot(
        temp_data[["date"]], temp_data[["max_temp"]], c="red", label="Max temperature"
    )
    ax.set_xlabel("Date")
    ax.set_ylabel("Temperature C")
    ax.set_axisbelow(True)
    ax.grid(True)
    ax.margins(x=0)
    ax.legend()

    fig.autofmt_xdate(rotation=45)

    if city_name is not None:
        ax.set_title(f"Weather in {city_name}")

    if today is not None:
        ax.axvline(x=today, c="black")

    fig.savefig(fname=f"{save_path}/weather_{city_name.lower()}.png")

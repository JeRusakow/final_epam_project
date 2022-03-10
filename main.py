"""
This is the main module of the app.
It contains main() and provides argument parsing
"""

import argparse
import asyncio
import os
import shutil
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from utils.async_utils import get_addresses, get_weather_bulk
from utils.dataframe_utils import (
    draw_and_save_temp_graph,
    find_max_temp_city,
    find_max_temp_delta_city,
    find_max_temp_diff,
    find_min_temp_city,
    refine_data,
    select_most_hoteled_cities,
)
from utils.file_utils import (
    assemble_dataframe,
    save_dataframe_as_csv_splitted,
    unpack_csv_from_zipfile,
)


def main():
    """This is the main method of the app"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_path",
        metavar="input_path",
        type=str,
        help="Path to ZIP archive with input data",
    )
    parser.add_argument(
        "output_path",
        metavar="output_path",
        type=str,
        help="Path to output directory",
    )
    parser.add_argument(
        "requests_per_second",
        metavar="request_rate",
        type=int,
        nargs="?",
        default=1,
        help="Number of requests sent per second while getting geocoding data",
    )

    args = parser.parse_args()

    input_file = Path(args.input_path)
    output_dir = Path(args.output_path)
    requests_per_second = args.requests_per_second
    extraction_dir = output_dir / "temp"

    date_today = datetime.utcnow().date()

    # Error checking
    if not zipfile.is_zipfile(input_file):
        raise zipfile.BadZipfile(f"File '{input_file}' is not a proper ZIP")

    unpack_csv_from_zipfile(input_file, extraction_dir)

    main_dataframe = assemble_dataframe(extraction_dir)

    shutil.rmtree(extraction_dir)

    # Cleaning invalid data
    main_dataframe = refine_data(main_dataframe)

    # Searching cities with the most hotels
    most_hoteled_cities_df = select_most_hoteled_cities(main_dataframe)

    hotels_of_interest = pd.merge(
        most_hoteled_cities_df, main_dataframe, on=["Country", "City"]
    )

    # Computing city centers' coords
    city_coords = hotels_of_interest.groupby(["Country", "City"], as_index=False).agg(
        min_lat=("Latitude", min),
        max_lat=("Latitude", max),
        min_lon=("Longitude", min),
        max_lon=("Longitude", max),
    )
    city_coords["Latitude"] = city_coords[["max_lat", "min_lat"]].mean(axis=1)
    city_coords["Longitude"] = city_coords[["max_lon", "min_lon"]].mean(axis=1)

    most_hoteled_cities_df = pd.merge(
        most_hoteled_cities_df, city_coords, on=["Country", "City"]
    )[["Country", "City", "Latitude", "Longitude"]]

    # Fetching weather for cities
    weather_per_city = asyncio.run(
        get_weather_bulk(most_hoteled_cities_df[["Latitude", "Longitude"]].values)
    )

    # Cropping weather data with a 5-day window for history and forecast data
    weather_per_city = [
        city_weather[abs(city_weather["date"] - date_today) <= timedelta(days=5)]
        for city_weather in weather_per_city
    ]

    # Constructing a dictionary with temperature for each city
    weather_per_city = {
        (row["Country"], row["City"]): weather
        for weather, (_, row) in zip(
            weather_per_city, most_hoteled_cities_df.iterrows()
        )
    }

    # Computing and printing data statistics
    print("Max temperature")  # noqa: T001
    for _, row in find_max_temp_city(weather_per_city).iterrows():
        print(  # noqa: T001
            f"\t{row['City']} ({row['Country']}): {row['temp']:.2f} C at {row['date']}"
        )
    print("Min temperature")  # noqa: T001
    for _, row in find_min_temp_city(weather_per_city).iterrows():
        print(  # noqa: T001
            f"\t{row['City']} ({row['Country']}): {row['temp']:.2f} C at {row['date']}"
        )
    print("Max daily temperature difference")  # noqa: T001
    for _, row in find_max_temp_diff(weather_per_city).iterrows():
        print(  # noqa: T001
            f"\t{row['City']} ({row['Country']}): "
            f"{row['temp_diff']:.2f} C at {row['date']}"
        )
    print("Max temperature change over current period")  # noqa: T001
    for _, row in find_max_temp_delta_city(weather_per_city).iterrows():
        print(  # noqa: T001
            f"\t{row['City']} ({row['Country']}): {row['temp_delta']:.2f} C"
        )

    # Getting hotels' addresses
    addresses = asyncio.run(
        get_addresses(
            hotels_of_interest[["Latitude", "Longitude"]].values,
            req_per_sec=requests_per_second,
        )
    )
    hotels_of_interest["Address"] = addresses

    # Saving data
    for _, row in most_hoteled_cities_df.iterrows():
        save_dir = output_dir / f"{row['City']}_{row['Country']}"
        if not os.path.exists(save_dir):
            os.mkdir(save_dir)

        curr_city_hotels = hotels_of_interest[
            (hotels_of_interest["City"] == row["City"])
            & (hotels_of_interest["Country"] == row["Country"])
        ]
        curr_city_hotels.reset_index(inplace=True, drop=True)

        save_dataframe_as_csv_splitted(
            curr_city_hotels[["Name", "Address", "Latitude", "Longitude"]],
            save_dir,
            name_prefix="hotels",
        )

        draw_and_save_temp_graph(
            weather_per_city[(row["Country"], row["City"])],
            save_dir,
            f"{row['City']}_{row['Country']}",
            datetime.utcnow().date(),
        )

        most_hoteled_cities_df[
            (most_hoteled_cities_df["City"] == row["City"])
            & (most_hoteled_cities_df["Country"] == row["Country"])
        ][["Latitude", "Longitude"]].to_csv(save_dir / "center_coords.csv", index=None)


if __name__ == "__main__":
    main()

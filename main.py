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
    refine_data,
    select_most_hoteled_cities,
)
from utils.file_utils import (
    assemble_dataframe,
    save_dataframe_as_csv_splitted,
    unpack_csv_from_zipfile,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_path", metavar="input_path", type=str, help="Path to ZIP archive"
    )
    parser.add_argument(
        "output_path",
        metavar="output_path",
        type=str,
        help="Path to the output directory",
    )
    parser.add_argument(
        "cores_number",
        metavar="j",
        type=int,
        nargs="?",
        default=1,
        help="Number of cores for multiprocessing use",
    )

    args = parser.parse_args()

    input_file = Path(args.input_path)
    output_dir = Path(args.output_path)
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

    # Drawing and saving temperature plots
    img_dir = output_dir / "img"
    if not os.path.exists(img_dir):
        os.mkdir(img_dir)
    for (country, city), weather in weather_per_city.items():
        draw_and_save_temp_graph(
            weather, img_dir, city_name=f"{city}_{country}", today=date_today
        )

    # Getting hotels' addresses
    addresses = asyncio.run(
        get_addresses(hotels_of_interest[["Latitude", "Longitude"]].values)
    )
    hotels_of_interest["Address"] = addresses

    # Saving data
    csv_save_dir = output_dir / "csv"
    if not os.path.exists(csv_save_dir):
        os.mkdir(csv_save_dir)
    save_dataframe_as_csv_splitted(
        hotels_of_interest[["Name", "Address", "Latitude", "Longitude"]],
        csv_save_dir,
        name_prefix="hotels",
    )


if __name__ == "__main__":
    main()

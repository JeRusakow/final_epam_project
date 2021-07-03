import argparse
import asyncio
import zipfile
from pathlib import Path

import pandas as pd

from utils.async_utils import get_addresses
from utils.dataframe_utils import refine_data, select_most_hoteled_cities
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

    # Error checking
    if not zipfile.is_zipfile(input_file):
        raise zipfile.BadZipfile(f"File '{input_file}' is not a proper ZIP")

    unpack_csv_from_zipfile(input_file, extraction_dir)

    main_dataframe = assemble_dataframe(extraction_dir)

    # Cleaning invalid data
    main_dataframe = refine_data(main_dataframe)

    # Searching cities with the most hotels
    most_hoteled_cities_df = select_most_hoteled_cities(main_dataframe)

    hotels_of_interest = pd.merge(
        most_hoteled_cities_df, main_dataframe, on=["Country", "City"]
    )

    # Getting hotels' addresses
    addresses = asyncio.run(
        get_addresses(hotels_of_interest[["Latitude", "Longitude"]].values)
    )
    hotels_of_interest["Address"] = addresses

    # Saving data
    save_dataframe_as_csv_splitted(
        hotels_of_interest[["Name", "Address", "Latitude", "Longitude"]],
        output_dir,
        name_prefix="hotels",
    )


if __name__ == "__main__":
    main()

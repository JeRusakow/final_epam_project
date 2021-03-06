""" This module contains functions for interaction with files"""

import zipfile
from os import PathLike
from pathlib import Path
from typing import Union, List

import pandas as pd


def unpack_csv_from_zipfile(
    zipfile_path: Union[str, PathLike], extract_dir: Union[str, PathLike]
) -> List[Union[str, PathLike]]:
    """
    Unpacks CSVs from a ZIP file into "temp" directory inside extract_dir.
    Directory extract_dir/temp must be cleaned up manually
    Args:
        zipfile_path: path to zipfile
        extract_dir: path to the directory where the files should be extracted to

    Returns:
        Paths to extracted files
    """
    if not zipfile.is_zipfile(zipfile_path):
        raise zipfile.BadZipfile(f"File '{zipfile_path}' is not a proper ZIP file")

    file_list = []
    with zipfile.ZipFile(zipfile_path) as zip_file:
        name_list = zip_file.namelist()
        for name in name_list:
            if name.endswith(".csv"):
                file_list.append(zip_file.extract(name, path=extract_dir))
    return file_list


def assemble_dataframe(csv_dir_path: Path) -> pd.DataFrame:
    """
    Concatenates all the CSVs in a directory
    Args:
        csv_dir_path: a directory with ONLY CSV files

    Returns:
        DataFrame of all the CSVs
    """
    subframes = [pd.read_csv(filename) for filename in csv_dir_path.iterdir()]
    return pd.concat(subframes)


def save_dataframe_as_csv_splitted(
    dataframe: pd.DataFrame, dest_dir: PathLike, name_prefix="csv", chunk_size=100
):
    """
    Saves a dataframe as CSV to dest_dir splitting it into chunks
    Args:
        dataframe: A dataframe to be saved
        dest_dir: A directory where save the data to
        name_prefix: Common name prefix for all CSV chunks
        chunk_size: A length of each chunk

    Returns:
        None
    """

    for idx, row_cnt in enumerate(range(0, len(dataframe), chunk_size)):
        tmp_chunk = dataframe[row_cnt : row_cnt + chunk_size]
        chunk_path = f"{dest_dir}/{name_prefix}_{idx:04d}.csv"
        tmp_chunk.to_csv(chunk_path)

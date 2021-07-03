import numpy as np
import pandas as pd


def refine_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """

    Args:
        dataframe:

    Returns:

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

    Args:
        dataframe:

    Returns:

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

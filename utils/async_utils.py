import asyncio
import json
from datetime import date, datetime, timedelta
from typing import Generator, Iterable, Union

import aiohttp
import geopy as gp
import pandas as pd
from geopy.extra.rate_limiter import AsyncRateLimiter

from api_keys import here_api_key, weathermap_api_key


async def get_adress_by_coordinates(
    lat: float, lon: float, rev_geoloc
) -> Union[str, None]:
    """
    Retrieves an address of a single object by its coordinates
    Args:
        lat: object latitude
        lon: object longitude
        rev_geoloc: a coroutine to be called for basic operation

    Returns:
        Address
    """
    location = await rev_geoloc(f"{lat}, {lon}")
    return location.address


async def get_addresses(coords: Iterable) -> list[Union[str, None]]:
    """
    Retrieves a bunch of addresses using HERE geocoding API
    Args:
        coords: A collection of pairs latitude-longitude

    Returns:
        List of addresses
    """
    async with gp.geocoders.Here(
        apikey=here_api_key,
        user_agent="wheather_monitoring",
        adapter_factory=gp.adapters.AioHTTPAdapter,
    ) as geolocator:
        reverse = AsyncRateLimiter(geolocator.reverse, min_delay_seconds=1 / 32)
        return await asyncio.gather(
            *[get_adress_by_coordinates(lat, lon, reverse) for lat, lon in coords]
        )


async def make_request(req: str, session: aiohttp.ClientSession) -> json:
    """
    A simple routine for sending single HTTP request.
    Args:
        req: HTTP address
        session: session object

    Returns:
    HTTP request result as JSON object
    """
    async with session.get(req) as response:
        return await response.json()


async def get_weather(
    lat: float, lon: float, session: aiohttp.ClientSession, history_depth=4
) -> pd.DataFrame:
    """
    Acquires history and forecasted weather from openweathermap.org for a place
    defined with latitude and longitude.

    ATTENTION!
    Due to 5-day limitation of history data it's impossible to get data for the
    whole 5-th (starting from 00:00 UTC) day before today, so it is not included.
    However, this parameter can be adjusted.
    Args:
        lat: latitude of a place
        lon: longitude of a place
        history_depth: the depth of history data to be fetched.
        session: a HTTP session object

    Returns:
    DataFrame of three columns: "date", "max_temp", "min_temp". Index column
    contains day number relatively to today, so 0 means today, negative
    numbers refer to the past and positive ones to the future.
    """
    req_prefix = "https://api.openweathermap.org/data/2.5/onecall"
    exclude_part = "minutely,hourly,alerts,current"
    date_today = datetime.utcnow().date()
    curr_and_fore_req = (
        f"{req_prefix}?lat={lat}&lon={lon}&exclude={exclude_part}"
        f"&appid={weathermap_api_key}&units=metric"
    )

    history_timestamps = (
        int(h_date.timestamp())
        for h_date in date_range(date_today - timedelta(days=history_depth), date_today)
    )
    history_reqs = [
        f"{req_prefix}/timemachine?lat={lat}&lon={lon}&dt={his_timestamp}&"
        f"appid={weathermap_api_key}&units=metric"
        for his_timestamp in history_timestamps
    ]

    curr_json = await make_request(curr_and_fore_req, session)
    history_jsons = await asyncio.gather(
        *[make_request(his_req, session) for his_req in history_reqs]
    )

    curr_and_forecasted_weather = parse_forecasted_data(curr_json)
    history_weather = pd.DataFrame(map(parse_historic_data, history_jsons))
    return pd.concat([history_weather, curr_and_forecasted_weather])


async def get_weather_bulk(coords: Iterable, history_depth=4) -> list[pd.DataFrame]:
    """
    An adapter function for asynchronously calling "get_weather" for several locations
        at once.
    Args:
        coords: an Iterable object, containing pairs of longitude and latitude of
            places.

    Returns:
    List of DataFrames containing weather info for each place
    """
    async with aiohttp.ClientSession() as session:
        return await asyncio.gather(
            *[
                get_weather(lat, lon, session, history_depth=history_depth)
                for lat, lon in coords
            ]
        )


def parse_forecasted_data(data: json) -> pd.DataFrame:
    """
    Parses JSON of weather forecast from openweathermap.org
    Args:
        data: JSON gotten as request result

    Returns:
    DataFrame with date, max and min temperatures for all dates found in JSON
    """
    temps = []
    for day_data in data["daily"]:
        curr_date = datetime.fromtimestamp(day_data["dt"]).date()
        min_temp = day_data["temp"]["min"]
        max_temp = day_data["temp"]["max"]
        temps.append({"date": curr_date, "min_temp": min_temp, "max_temp": max_temp})
    return pd.DataFrame(temps)


def parse_historic_data(data: json) -> pd.Series:
    """
    Parses JSON of weather history from openweathermap.org.
    Since history data can be queried only per single day at once, this function
    returns a Series object instead of DataFrame.
    Args:
        data: the JSON gotten as a request result

    Returns:
    A Series object containing date, max and min temperature.
    """
    temps = []
    curr_date = datetime.fromtimestamp(data["current"]["dt"]).date()
    for hour_data in data["hourly"]:
        temps.append(hour_data["temp"])
    return pd.Series(
        {"date": curr_date, "max_temp": max(temps), "min_temp": min(temps)},
        name=(curr_date - datetime.utcnow().date()).days,
    )


def date_range(
    start: Union[date, datetime], stop: Union[date, datetime]
) -> Generator[datetime, None, None]:
    """
    Generates date range. Datetime objects being yielded are set to OO:00:00 UTC.
    Args:
        start: start date
        stop: stop date, not yielded

    Yields:
    Datetime objects for given interval.
    """
    for days in range(start.toordinal(), stop.toordinal()):
        yield datetime.fromordinal(days)


if __name__ == "__main__":
    d = asyncio.run(get_weather(48.204, 16.351))
    print(d)  # noqa

import asyncio
from typing import Iterable, Union

import geopy as gp
from geopy.extra.rate_limiter import AsyncRateLimiter

from api_keys import here_api_key


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


if __name__ == "__main__":
    coords = [
        (59.941263, 30.350687),
        (59.940222, 30.257285),
        (59.934793, 30.384080),
        (59.924702, 30.299862),
    ]

    adr = asyncio.run(get_addresses(coords))
    print(adr)  # noqa: T001

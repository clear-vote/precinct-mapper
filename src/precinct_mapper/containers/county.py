from __future__ import annotations
from shapely.geometry import MultiPolygon, Polygon, Point
from . import Region, Precinct
from geopandas import GeoDataFrame


class County(Region):
    """A class to store data on a county and the precincts contained within it."""

    def __init__(
        self, name: str, boundary: Polygon | MultiPolygon, identifier: int | None = None
    ):
        super().__init__("county", name, boundary, identifier)
        self.precincts = GeoDataFrame(
            columns=["precinct", "geometry"],
            geometry="geometry",
        )

    def get_precinct(self, coord: Point):
        matching_rows = self.precincts.loc[self.precincts.contains(coord)]
        num_matching = len(matching_rows)
        if num_matching == 0:
            raise ValueError(
                f"Given coordinates {coord} do not map to a known precinct."
            )
        elif num_matching > 1:
            raise ValueError(
                f"Given coordinates map to {num_matching} precincts ({[p.identifier for p in matching_rows['precinct']]})"
            )

        return matching_rows.iloc[0]["precinct"]

    def add_precinct(self, precinct: Precinct):
        self.precincts.loc[len(self.precincts)] = [precinct, precinct.boundary]

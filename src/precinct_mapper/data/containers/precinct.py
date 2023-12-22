from shapely.geometry import MultiPolygon, Polygon
from . import Region


class Precinct(Region):
    def __init__(
        self, name: str, boundary: Polygon | MultiPolygon, identifier: int | None = None
    ):
        super().__init__(name, boundary, identifier)
        self.maps_to = {}  # map of boundary type to boundary obj

    def add_containing_boundary(self, boundary: Region):
        self.maps_to[boundary.type] = boundary

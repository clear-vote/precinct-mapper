from __future__ import annotations
from shapely.geometry import Point, Polygon, MultiPolygon


class Region:
    """A base class to store the name and geographical boundary of some region."""
    
    def __init__(
        self,
        btype: str,
        name: str,
        boundary: Polygon | MultiPolygon,
        identifier: None | int = None,
    ):
        """Initializes boundary object.

        Args:
            btype: string denoting type of boundary (e.g., city, county, etc.)
            name: name of the boundary
            boundary: shape of this region's boundary
            identifier (optional): a unique integer ID for this region to disntinguish
                it from other regions of the same btype
        """
        self.btype = btype
        self.name = name
        self.boundary = boundary
        self.identifier = identifier

    def contains(self, other: Region | Point | Polygon | MultiPolygon):
        """Returns True if this boundary contains other.

        Args:
            other: region or shape to check against
        """
        if isinstance(other, Region):
            return self.boundary.contains(other.boundary)
        elif (
            isinstance(other, Point)
            or isinstance(other, Polygon)
            or isinstance(other, MultiPolygon)
        ):
            return self.boundary.contains(other)
        raise TypeError(
            f"other must be one of: 'Region', 'Point', 'Polygon', 'MultiPolygon'. got: { type(other) }"
        )

    def __str__(self):
        """Returns a simple stringified version of this region containing
        type, name, and identifier."""
        return f"type: {self.type}, name: {self.name}, identifier: {self.identifier}"

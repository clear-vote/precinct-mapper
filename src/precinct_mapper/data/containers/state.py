from shapely.geometry import MultiPolygon, Polygon, Point
from . import Region, County, Precinct
from geopandas import GeoDataFrame
from typing import List

class State(Region):
    """A class to represent a state, its boundary, and counties."""
    def __init__(
        self, name: str, boundary: Polygon | MultiPolygon, additional_columns: List[str] = []
    ):
        """Initializes State object with btype of \'state\'.
        By default, 

        Args:
            name: name of the boundary
            boundary: shape of this state's boundary
        """
        super().__init__("state", name, boundary)
        self.geotable = GeoDataFrame(
            columns=[
                "precinct",
                "precinct_geometry",
                "legislative_district",
                "congressional_district",
                "school_district",
                "county",
                "city"
                ] + additional_columns,
                geometry="precinct_geometry"
            )

    def get_county_names(self) -> List[str]:
        """Returns the names of all counties in this state in ascending order"""
        return sorted(list(self.counties["name"]))
    
    def get_county_by_coordinate(self, coord: Point) -> County:
        """Returns the County that contains the given coordinates.
        
        Args:
            coord: coordinates to locate containing county of

        Raises:
            LookupError: if no county in this state contains the given coordinates
        """
        for county in self.counties:
            if county.contains(coord):
                return county
        raise LookupError(f"Could not find county containing coordinates: { coord }")

    def get_county_by_name(self, county_name: str) -> County:
        """Returns the County in this state with the given name"""
        if county_name not in self.counties["name"]:
            raise LookupError(f"Could not find county with name: { county_name }. Try one of {list(self.counties['name'])}")
        
        return self.counties.loc[self.counties["name"] == county_name].iloc[0]

    def get_precinct(self, coord: Point) -> Precinct:
        county = self.get_county()
        return county.get_precinct(coord)

    def add_county(self, county: County):
        if county.name in self.counties["name"]:
            raise ValueError(f"Given county ('{ county }') is already in this state")
        self.counties[len(self.counties)] = [county.name, county, county.boundary]

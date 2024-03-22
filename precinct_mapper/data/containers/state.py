from __future__ import annotations
import pickle
from math import isnan
from shapely.geometry import Point
from . import Region
from geopandas import GeoDataFrame
from typing import Dict
from pathlib import Path

class State:
    """A class to represent a state that the client can request district info from."""
    
    def __init__(self, tables: Dict[str, GeoDataFrame], precinct_lookup_table: GeoDataFrame):
        """Initializes State object with the given region and lookup tables.

        Args:
            tables: a dictionary mapping a btype (e.g., \"school_district\")
                    to its boundaries
            precinct_lookup_table: a geo dataframe where the \"
        """
        self.tables = tables
        self.lookup_table = precinct_lookup_table
        self.btypes = list(tables.keys())

    @staticmethod
    def from_cache(filepath: str | Path) -> State:
        """Loads a state object from the given pickled State object.
        
        Args:
            filepath: path to pickled file.
        
        Returns:
            state object from unpickling the file with the given filepath.
        """
        with open(filepath, "rb") as f:
            s = pickle.load(f)
        return s
    
    def to_cache(self, filepath: str | Path):
        """Pickles the current state object at the given filepath.
        
        Args:
            filepath: path to write pickle.
        """
        with open(filepath, "wb") as f:
            pickle.dump(self, f)

    def lookup_lat_lon(self, lat: float, lon: float) -> Dict[str, Region]:
        """Looks up the given (latitude, longitude) """
        pt = Point(lat, lon)
        return self.lookup_point(pt)
    
    def lookup_point(self, pt: Point):
        lookup_result = self.lookup_table.loc[self.lookup_table.contains(pt)]
        if len(lookup_result) == 0:
            raise LookupError(f"Could not find precinct for coordinates:\n" + \
                              f"Latitude: {pt.x}, Longitude: {pt.y}")
        elif len(lookup_result) > 1:
            raise LookupError(f"Too many precincts found for coordinates:\n" + \
                              f"Latitude: {pt.x}, Longitude: {pt.y}")
        # successful lookup, retrieve all boundary info
        single_result = lookup_result.iloc[0]
        boundary_info = {}
        boundary_info["precinct"] = Region("precinct",
                                            str(single_result["id"]),
                                            single_result["geometry"],
                                            single_result["id"])
        for btype in self.btypes:
            btype_id = single_result[btype]
            if isnan(btype_id) or btype_id is None:
                boundary_info[btype] = None
            else:
                boundary_info[btype] = self._lookup_btype_id(btype, int(btype_id))
        return boundary_info
    
    def _lookup_btype_id(self, btype: str, id: int):
        if btype not in self.btypes:
            raise ValueError(f"Boundary type {btype} is not one of {self.btypes}.")
        btable = self.tables[btype]
        try:
            result = btable.iloc[id]
        except IndexError as ie:
            raise LookupError(f"Could not find {btype} with id {id}. Found {len(result)}.") from ie
        return Region(btype, result["name"], result["geometry"], identifier=result["id"])

from __future__ import annotations
import fiona
import pickle
import geopandas as gpd
from math import isnan
from shapely.geometry import Point
from . import Region
from json import dump
from typing import Dict
from pathlib import Path

class State:
    """A class to represent a state that the client can request district info from."""
    
    def __init__(self, tables: Dict[str, gpd.GeoDataFrame], precinct_lookup_table: gpd.GeoDataFrame):
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
            
        Raises:
            FileNotFoundError: if directory of requested filepath does not exist.
        """
        with open(filepath, "wb") as f:
            pickle.dump(self, f)
            
    def to_dir(self, path: str | Path):
        """Writes the current state object at the given path as a directory
        with the following layout:
        ```dirpath/
        |--metadata.json
        |--data.gpkg```
        
        If the requested directory or its parent directories do not exist,
        they will be created.
        
        Args:
            filepath: path to write pickle.
            
        Raises:
            FileNotFoundError: if directory of requested filepath does not exist.
        """
        if isinstance(path, str):
            path = Path(path)
        
        if not path.exists():
            path.mkdir(parents=True) # make parents and self
        
        metadata_path = path / "metadata.json"
        data_path = path / "data.gpkg"
        
        # not super necessary right now; could be expanded later
        metadata = {
            "boundary_types" : self.btypes
        }
        
        with open(metadata_path, "w") as metadata_file:
            dump(metadata, metadata_file)
        
        lookup_table_shallow_copy = self.lookup_table.copy(deep=False)
        lookup_table_shallow_copy["index"] = lookup_table_shallow_copy.index
        lookup_table_shallow_copy.to_file(data_path, layer="precinct", driver="GPKG")
        
        for btype, table in self.tables.items():
            table_shallow_copy = table.copy(deep=False)
            table_shallow_copy["index"] = table_shallow_copy.index
            table_shallow_copy.to_file(data_path, layer=btype, driver="GPKG")
    
    @staticmethod
    def from_dir(path: str | Path) -> State:
        if isinstance(path, str):
            path = Path(path)
        
        if not path.exists():
            raise ValueError(f"Given path '{str(path)}' does not exist")

        if not path.is_dir():
            raise ValueError(f"Given path '{str(path)}' is not a directory")
        
        metadata_path = path / "metadata.json"
        data_path = path / "data.gpkg"
        
        if not (metadata_path.exists() and metadata_path.is_file()):
            raise ValueError(f"Could not find metadata.json in {str(path)}")

        if not (data_path.exists() and data_path.is_file()):
            raise ValueError(f"Could not find data.gpkg in {str(path)}")
        
        layers = fiona.listlayers(data_path)
        if "precinct" not in layers:
            raise RuntimeError(f"data.gpkg does not include required layer 'precinct'")
        layers.remove("precinct")
        lookup_table = State._read_layer_and_index(data_path, "precinct")
        
        tables = {}
        for layer_name in layers:
            tables[layer_name] = State._read_layer_and_index(data_path, layer_name)
        
        return State(tables, lookup_table)

    @staticmethod
    def _read_layer_and_index(path: Path, layer: str) -> gpd.GeoDataFrame:
        table = gpd.read_file(path, layer=layer)
        if "index" not in table.columns:
            raise RuntimeError(f"{ layer } layer does not contain index column")
        table.set_index("index", inplace=True, drop=True)
        return table

    
    def lookup_lat_lon_as_dict(self, lat: float, lon: float) -> Dict[str, Dict[str, any]]:
        regions_result = self.lookup_lat_lon(lat, lon)
        result = {}
        for btype, region in regions_result.items():
            if region is None:
                result[btype] = None
            else:
                result[btype] = region.as_dict()
        return result
        
    def lookup_lat_lon_as_dict(self, lat: float, lon: float) -> Dict[str, Dict[str, any]]:
        regions_result = self.lookup_lat_lon(lat, lon)
        result = {}
        for btype, region in regions_result.items():
            if region is None:
                result[btype] = None
            else:
                result[btype] = region.as_dict()
        return result
    
    def lookup_lat_lon(self, lat: float, lon: float) -> Dict[str, Region]:
        """Looks up the given (latitude, longitude) """
        pt = Point(lat, lon)
        return self._lookup_point(pt)
    
    def _lookup_point(self, pt: Point): # pt = coord as (lat, long)
        long_lat = Point(pt.y, pt.x)
        lookup_result = self.lookup_table.loc[self.lookup_table.contains(long_lat)]
        if len(lookup_result) == 0:
            raise LookupError(f"Could not find precinct for coordinates:\n" + \
                              f"Latitude: {long_lat.y}, Longitude: {long_lat.x}")
        elif len(lookup_result) > 1:
            raise LookupError(f"Too many precincts found for coordinates:\n" + \
                              f"Latitude: {long_lat.y}, Longitude: {long_lat.x}")
        # successful lookup, retrieve all boundary info
        single_result = lookup_result.iloc[0]
        boundary_info = {}
        boundary_info["precinct"] = Region("precinct",
                                           single_result.get("name"),
                                           single_result.get("geometry"),
                                           single_result.get("id"))
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

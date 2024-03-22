from __future__ import annotations
import shutil
import geopandas as gpd
import pandas as pd
from pathlib import Path
from shapely.geometry.point import Point
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from typing import Collection, Dict, List
from typeguard import typechecked
from precinct_mapper.data.containers import Region, State

@typechecked
class StateParser:
    datapath = Path(__file__).parent / "datasets"

    def __init__(self, code: str):
        if len(code) != 2:
            raise ValueError(f"State codes must be two letters. Got: \'{ code }\'")
        self.code = code.upper()
        self.state_all_datapath = StateParser.datapath / self.code
        assert self.state_all_datapath.is_dir()
        self.state_tables_datapath = self.state_all_datapath / "state"
        assert self.state_tables_datapath.is_dir()
        self.region_tables_datapath = self.state_all_datapath / "region_tables"
        if not self.region_tables_datapath.exists():
            self.region_tables_datapath.mkdir()
        assert self.region_tables_datapath.is_dir()
        self.precinct_filepath = self.state_tables_datapath / "precinct.gpkg"
        if not self.precinct_filepath.exists():
            raise FileNotFoundError(f"Precincts file not found at { self.precinct_filepath }. Ensure it has been fetched")
    
    @staticmethod
    def _process_holes(shape: Polygon | MultiPolygon):
        if isinstance(shape, Polygon):
            return shape
        elif isinstance(shape, MultiPolygon):
            polys = shape.geoms
            exts = []
            holes = []

            for p in polys:
                if p.exterior.is_ccw:
                    holes.append(p)
                else:
                    exts.append(p)
            
            out_polys = []
            for e in exts:
                for h in holes:
                    if h.within(e):
                        e = e.difference(h)
                out_polys.append(e)
            return MultiPolygon(out_polys)

    def parse(self, recompile: bool = False) -> State:
        if recompile:
            self._invert_regional_data()

        state_tables = StateParser._read_directory_tables(self.state_tables_datapath,
                                                          exclude=["precinct"])
        region_tables = StateParser._read_directory_tables(self.region_tables_datapath)
        all_tables = state_tables | region_tables

        precincts = gpd.read_file(self.precinct_filepath)
        precincts["geometry"] = precincts["geometry"].apply(StateParser._process_holes)

        precincts_filled = precincts
        for btype, binfo in all_tables.items():
            precincts_filled[btype] = precincts_filled.apply(lambda row: StateParser._get_bounding_region_index(row["geometry"], binfo), axis=1)
        
        return State(all_tables, precincts_filled)
            
    def _invert_regional_data(self):
        shutil.rmtree(self.region_tables_datapath)
        self.region_tables_datapath.mkdir()
        for scope in self.state_all_datapath.iterdir():
            if scope.is_dir() and scope.stem not in ("state", "region_tables"):
                for region_name in scope.iterdir():
                    if region_name.is_dir():
                        for boundary in region_name.glob("*.gpkg"):
                            boundary_outpath = self.region_tables_datapath / f"{ boundary.stem }.gpkg"
                            table = None
                            new_boundary_table = gpd.read_file(boundary)
                            new_boundary_table["region_name"] = region_name.stem
                            new_boundary_table["scope"] = scope.stem
                            if boundary_outpath.exists():
                                existing_table = gpd.read_file(boundary_outpath)
                                tables = [existing_table, new_boundary_table]
                                table = gpd.GeoDataFrame(pd.concat(tables, ignore_index=True), crs=tables[0].crs)
                            else:
                                table = new_boundary_table
                            table.to_file(boundary_outpath)

    @staticmethod
    def _read_directory_tables(dirpath: str | Path,
                               filepattern: str = "*.gpkg",
                               exclude: Collection[str] = []) -> Dict[str, gpd.GeoDataFrame]:
        if isinstance(dirpath, str):
            dirpath = Path(dirpath)
        if not (dirpath.exists() and dirpath.is_dir()):
            raise ValueError("Given directory path is not valid")
        tables = {}
        for file in dirpath.glob(filepattern):
            if file.stem not in exclude:
                table = gpd.read_file(file)
                table["geometry"] = table["geometry"].apply(StateParser._process_holes)
                tables[file.stem] = table
        return tables
    
    @staticmethod
    def _read(filepath: Path):
        if not filepath.is_file():
            raise ValueError(f"Given path must be for a file. Got { filepath }")
        
        match filepath.suffix:
            case ".gpkg":
                return gpd.read_file(filepath, layer=filepath.stem)
            case ".pkl":
                return pd.read_pickle(filepath)
            case _:
                return gpd.read_file(filepath)
    
    @staticmethod
    def _validate_boundary_results(results: gpd.GeoDataFrame, point: Point) -> Region | None:
        nrows = len(results)
        if nrows == 0:
            return None
        elif nrows > 1:
            results.plot(figsize=(15, 12), color=["lightblue", "purple"], edgecolor="black", alpha=0.2)
            print(results.iloc[0]["geometry"].contains(results.iloc[1]["geometry"]), results.iloc[0]["geometry"].within(results.iloc[1]["geometry"]))
            raise RuntimeError(f"Multiple boundaries contained {point}: {results['region']}.")
        
        return results.iloc[0]["region"]
            
    @staticmethod
    def _get_bounding_region_of_point(point: Point, regions_table: gpd.GeoDataFrame) -> Region | None:
        containing = regions_table.loc[regions_table.contains(point)]
        return StateParser._validate_boundary_results(containing, point)

    @staticmethod
    def _get_bounding_region(shape: Point | Polygon | MultiPolygon, regions_table: gpd.GeoDataFrame) -> Region | None:
        if not ("geometry" in regions_table and "region" in regions_table):
            raise ValueError(f"Given regions table is missing one of the columns [\'geometry\', \'region\']. Given columns: {list(regions_table.columns)}")

        if isinstance(shape, Point):
            return StateParser._get_bounding_region_of_point(shape, regions_table)
        elif isinstance(shape, MultiPolygon):
            polygon = max(shape.geoms, key = lambda p: p.area) # get the largest polygon
        elif isinstance(shape, Polygon):
            polygon = shape
        else:
            raise ValueError(f"shape type { type(shape) } is not valid.")
        point = polygon.centroid
        if not point.within(polygon):
            point = polygon.representative_point()
        return StateParser._get_bounding_region_of_point(point, regions_table)

    @staticmethod                                   
    def _row_to_region(btype: str, column_names: pd.Index, row: pd.core.series.Series | gpd.GeoSeries) -> Region:
        metadata = {}
        for col in [col for col in column_names if col not in {"name", "geometry", "id"}]:
            metadata[col] = row.get(col, None)

        return Region(
            btype,
            row.get("name", None),
            row.get("geometry", None),
            row.get("id", None),
            metadata=metadata
        )

    @staticmethod
    def _extract_value(key: str, column_names: pd.Index, row: gpd.GeoSeries):
        if key in column_names:
            return row[column_names]
        return None

    @staticmethod
    def _validate_boundary_index_results(results: List[int], binfo: gpd.GeoDataFrame, point: Point) -> int | None:
        nrows = len(results)
        if nrows == 0:
            return None
        elif nrows > 1:
            multi_match_rows = binfo.iloc[results]
            raise RuntimeError(f"Multiple boundaries contained {point}: {multi_match_rows}.")
        return results[0]
    
    @staticmethod   
    def _get_bounding_region_index_point(point: Point, binfo: gpd.GeoDataFrame) -> int | None:
        containing = binfo.index[binfo.contains(point)].to_list()
        return StateParser._validate_boundary_index_results(containing, binfo, point)

    @staticmethod
    def _get_bounding_region_index(shape: Point | Polygon | MultiPolygon, binfo: gpd.GeoDataFrame) -> int | None:
        if not "geometry" in binfo:
            raise ValueError(f"Given regions table is missing one of the columns [\'geometry\']. Given columns: {list(binfo.columns)}")

        if isinstance(shape, Point):
            return StateParser._get_bounding_region_index_point(shape, binfo)
        elif isinstance(shape, MultiPolygon):
            polygon = max(shape.geoms, key = lambda p: p.area) # get the largest polygon
        elif isinstance(shape, Polygon):
            polygon = shape
        else:
            raise ValueError(f"shape type { type(shape) } is not valid.")
        point = polygon.centroid
        if not point.within(polygon):
            point = polygon.representative_point()
    
        return StateParser._get_bounding_region_index_point(point, binfo)

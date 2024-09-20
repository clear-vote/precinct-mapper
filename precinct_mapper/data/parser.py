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
    def __init__(self,
                 datapath: str | Path,
                 output_dir: str | Path,
                 primary_table_name: str = "precinct"):
        # TODO: complete documentation
        if isinstance(datapath, str):
            datapath = Path(datapath)
        if not (datapath.exists() and datapath.is_dir()):
            raise ValueError(f"Given datapath { datapath } does not exist or is not a directory.")
        
        if isinstance(output_dir, str):
            output_dir = Path(output_dir)
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)
        
        self.datapath = datapath
        self.output_dir = output_dir
        self.primary_table_path = next(self.datapath.rglob(f"{ primary_table_name }.*"), None)
        if self.primary_table_path is None:
            raise FileNotFoundError(f"No file named { primary_table_name }.* exists in the directory { datapath } or its children.")

    def parse(self, recompile: bool = False) -> State:
        if recompile:
            self._invert_regional_data()

        # state_tables = StateParser._read_directory_tables(self.state_tables_datapath,
        #                                                   exclude=["precinct"])
        tables = StateParser._read_directory_tables(self.output_dir, exclude=[self.primary_table_path.stem])
        # all_tables = state_tables
        # all_tables.update(region_tables)

        primary_table = gpd.read_file(self.primary_table_path)

        primary_table_filled = primary_table
        for btype, binfo in tables.items():
            print(f"Parsing btype: { btype }")
            primary_table_filled[btype] = primary_table_filled.apply(lambda row: StateParser._get_bounding_region_index(row["geometry"], binfo), axis=1)
        
        return State(tables, primary_table_filled)
            
    def _invert_regional_data(self):
        shutil.rmtree(self.output_dir)
        self.output_dir.mkdir()
        for scope in self.datapath.iterdir():
            if scope.is_dir() and scope.stem not in ("region_tables"):
                for region_name in scope.iterdir():
                    if region_name.is_dir():
                        print(f"Region { region_name } has files: [{ [g for g in region_name.glob('*.*')] }]")
                        for boundary in region_name.glob("*.*"):
                            print(f"Inverting: { scope }, { region_name }, { boundary }")
                            boundary_outpath = self.output_dir / f"{ boundary.stem }.gpkg"
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
        print(f"Excluding: { exclude }")
        if isinstance(dirpath, str):
            dirpath = Path(dirpath)
        if not (dirpath.exists() and dirpath.is_dir()):
            raise ValueError("Given directory path is not valid")
        tables = {}
        for file in dirpath.glob(filepattern):
            if file.stem not in exclude:
                print(f"File: {file}")
                table = gpd.read_file(file)
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

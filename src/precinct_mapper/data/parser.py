import geopandas as gpd
import pandas as pd
from pathlib import Path
from shapely.geometry.point import Point
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
# import Polygon, MultiPolygon, Point
from . import Region
import matplotlib.pyplot as plt
# from typeguard import typechecked

# @typechecked
class StateParser:
    datapath = Path(__file__).parent / "datasets"

    def __init__(self, code: str):
        if len(code) != 2:
            raise ValueError(f"State codes must be two letters. Got: '{ code }'")
        self.code = code.upper()
        self.state_datapath = StateParser.datapath / self.code

        self.precinct_filepath = self.state_datapath / "state" / "precinct.geojson"
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


    def parse(self) -> gpd.GeoDataFrame:
        datatable = gpd.read_file(self.precinct_filepath)
        datatable["geometry"] = datatable["geometry"].apply(StateParser._process_holes)
        datatable["precinct"] = datatable.apply(
            lambda row: StateParser._row_to_region("precinct", datatable.columns, row),
            axis=1
        )
        datatable = datatable[["geometry", "precinct"]]
        full_state_datapath = self.state_datapath / "state"
        for file in full_state_datapath.iterdir():
            if file.stem != "precinct":
                cur_table = gpd.read_file(file)
                cur_table["geometry"] = cur_table["geometry"].apply(StateParser._process_holes)
                cur_table["region"] = cur_table.apply(
                    lambda row: StateParser._row_to_region(file.stem, cur_table.columns, row),
                    axis=1
                )
                region_table = cur_table[["geometry", "region"]]
                datatable[file.stem] = datatable.apply(
                    lambda row: StateParser._get_bounding_region(row["geometry"], region_table),
                    axis=1
                )
        print()
        print("DONE FETCHING STATE LEVEL")
        print()
        for scope_dir in self.state_datapath.iterdir():
            if scope_dir.is_dir() and scope_dir.name != "state":
                print(f"btype {scope_dir}")
                for region_dir in scope_dir.iterdir():
                    print(f"\tregion {region_dir}")
                    if region_dir.is_dir():
                        region_rows = datatable[datatable[scope_dir.name].apply(lambda region: False if region is None else region.name == region_dir.name)]
                        for btype_file in region_dir.iterdir():
                            if btype_file.is_file() and btype_file.suffix == ".geojson":
                                btype = btype_file.stem
                                print(f"\t\t{btype_file.absolute}")
                                with open(btype_file, "r") as layer_file:
                                    layer_table = gpd.read_file(layer_file)
                                    layer_table["geometry"] = layer_table["geometry"].apply(StateParser._process_holes)
                                    layer_table["region"] = layer_table.apply(
                                        lambda row: StateParser._row_to_region(btype, layer_table.columns, row),
                                        axis=1
                                    )
                                    # print(layer_table.info())
                                    boundaries = region_rows.apply(
                                        lambda row: StateParser._get_bounding_region(row["geometry"], layer_table),
                                        axis=1
                                    )
                                    # print(boundaries)
                                    if btype not in datatable.columns:
                                        datatable[btype] = None
                                    datatable[btype].fillna(boundaries, inplace=True)
                                    # datatable.merge(boundaries, left_index=True, right_index=True, how="left")
                                    
        return datatable
    
    @staticmethod
    def _validate_boundary_results(results: gpd.GeoDataFrame, point: Point) -> Region | None:
        nrows = len(results)
        if nrows == 0:
            return None
        elif nrows > 1:
            results.plot(figsize=(15, 12), color=["lightblue", "purple"], edgecolor="black", alpha=0.2)
            # plt.scatter(point.x, point.y, marker="o", color="red", ax=ax)
            print(results.iloc[0]["geometry"].contains(results.iloc[1]["geometry"]), results.iloc[0]["geometry"].within(results.iloc[1]["geometry"]))
            raise RuntimeError(f"Multiple boundaries contained {point}: {results['region']}.")
        
        # print("boundary results")
        # print(results)
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
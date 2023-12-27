import geopandas as gpd
import pandas as pd
from pathlib import Path
from shapely import Polygon, MultiPolygon, Point
from . import Region
import matplotlib.pyplot as plt
from typeguard import typechecked

@typechecked
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
    

    def parse(self) -> gpd.GeoDataFrame:
        datatable = gpd.read_file(self.precinct_filepath)
        datatable["precinct"] = datatable.apply(
            lambda row: StateParser._row_to_region("precinct", datatable.columns, row),
            axis=1
        )
        datatable = datatable[["geometry", "precinct"]]
        full_state_datapath = self.state_datapath / "state"
        
        for file in full_state_datapath.iterdir():
            if file.stem != "precinct" and file.stem != "water_district":
                cur_table = gpd.read_file(file)

                cur_table["region"] = cur_table.apply(
                    lambda row: StateParser._row_to_region(file.stem, cur_table.columns, row),
                    axis=1
                )
                region_table = cur_table[["geometry", "region"]]
                datatable[file.stem] = datatable.apply(
                    lambda row: StateParser._get_bounding_region(row["geometry"], region_table),
                    axis=1
                )
        
        return datatable
    
    @staticmethod
    def _validate_boundary_results(results: gpd.GeoDataFrame, point: Point) -> Region | None:
        nrows = len(results)
        if nrows == 0:
            return None
        elif nrows > 1:
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

        if isinstance(shape, Polygon):
            return StateParser._get_bounding_region_of_point(shape.representative_point(), regions_table)
        if isinstance(shape, Point):
            return StateParser._get_bounding_region_of_point(shape, regions_table)
        if isinstance(shape, MultiPolygon):
            largest_polygon = max(shape.geoms, key = lambda p: p.area) # get the largest polygon
            return StateParser._get_bounding_region_of_point(largest_polygon.representative_point(), regions_table)
        else:
            raise ValueError(f"shape type { type(shape) } is not valid.")

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
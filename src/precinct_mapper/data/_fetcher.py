from __future__ import annotations
import requests
import json
import geopandas as gpd
from typing import List, Dict, NamedTuple
from pathlib import Path
from urllib.parse import urlencode
from requests import Request

class _DataFetcherHelper:
    datapath = Path(__file__).parent / "datasets"

    @staticmethod
    def _fetch(url: str, name: str, output_path: Path, overwrite_existing: bool = False):
        """Fetches the geoJSON from the given url and stores it as a geoJSON file
        under datasets/ using the given path extension.
        
        Args:
            url: web url to fetch data from.
            name: name stem of file to create (e.g., `\'city\'` for `\'city.geojson\'`).
            path: list of sequentially nested subdirectory names under `data/datasets/`
        
        Raises:
            RuntimeError: if issue accessing the given url for data.
        """
        output_path = output_path / f"{name}.geojson"
        if overwrite_existing or not output_path.exists():
            data = _DataFetcherHelper._get_boundaries(url)
            with open(output_path, "w") as file:
                file.write(data.to_json())
        
    @staticmethod
    def _nested_path(base: Path, subdirs: List[str], make_if_absent: bool = False) -> Path:
        """Returns the path found by accessing nested child subdirectories from the base.
        
        For example, given base: \'~/\' and subdirs [\'Users\', \'Voter\'], the Path of the
        directory corresponding to `~/Users/Voter/` would be returned.

        Args:
            base: path to base directory
            subdirs: list of names of nested child subdirectories
        
        Raises:
            FileNotFoundError: if base directory or any child directory does not exist.
        """
        _DataFetcherHelper._not_exists_handler(
            base,
            make_if_absent=make_if_absent,
            not_exists_message=f"Given base directory does not exist: {base.absolute()}"
        )
        output_path = base

        for subdir in subdirs:
            output_path /= subdir

            _DataFetcherHelper._not_exists_handler(
                output_path,
                make_if_absent=make_if_absent,
                not_exists_message=f"Child directory access failed. Directory does not exist: {output_path.absolute()}"
            )
        
        return output_path
    
    @staticmethod
    def _not_exists_handler(path: Path, make_if_absent: bool = False, not_exists_message: str = ""):
        """Handles the case where given directory path does not exist. If make_if_absent,
        makes directories that do no exist including parents'. Else, throw an error
        with not_exists_message.
        
        Args:
            path: path to a directory
            make_if_absent: if true, makes directories that do no exist including parents'.
            not_exists_message: if not make_if_absent, message for FileNotFoundError

        Raise:
            FileNotFoundError: if make_if_absent is False and the given path does not exist.
        """
        if not path.exists():
            if make_if_absent:
                path.mkdir()
            else:
                raise FileNotFoundError(not_exists_message)

    @staticmethod
    def _get_boundaries(url: str, timeout: int = 10) -> gpd.GeoDataFrame:
        """Returns a GeoPandas GeoDataFrame of the location data found by
        querying the given URL. Will give up after the given timeout
        in seconds.

        Args:
            url: web url to query
            timeout: an integer number of seconds to wait for a response
                before quitting.
        
        Raises:
            RuntimeError: if could not connect to host with the given url or

                request timed out or another request error was faced
        """
        try:
            data_request = requests.get(
                url,
                timeout=timeout
                )
            if not data_request.ok:
                raise RuntimeError("Could not query Open Data Portal")
            raw_boundary_data = json.loads(data_request.content)
            processed_boundary_data = gpd.GeoDataFrame.from_features(raw_boundary_data["features"])
            return processed_boundary_data
        except requests.exceptions.ConnectionError as exc:
            raise RuntimeError("Could not connect.") from exc
        except requests.exceptions.Timeout as exc:
            raise RuntimeError("Request timed out.") from exc
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(f"An error occurred: {exc}") from exc
        
class _BoundaryRequest:
    def __init__(self, name: str, base_url: str, out_fields: List[str]):
        self.name = name
        self.req = Request(
            url=base_url,
            params= {
                "where": "1=1",
                "outFields": ",".join(out_fields),
                "f": "geojson"
            }
        )

    def prepare_url(self):
        return self.req.prepare().url

class StateDataFetcher:
    def __init__(
            self,
            code,
            full_state_layer_requests: List[_BoundaryRequest],
            additional_layers: Dict[str, Dict[str, List[_BoundaryRequest]]]
        ):
        self.code = code
        self.state_output_path = _DataFetcherHelper.datapath / code
        self.state_output_path.mkdir(parents=True, exist_ok=True)
        self.full_state_layer_requests = full_state_layer_requests
        self.additional_layers = additional_layers

    def fetch(self, overwrite_existing):
        state_layers_output_path = self.state_output_path / "state"
        for req in self.full_state_layer_requests:
            url = req.prepare_url()
            _DataFetcherHelper._fetch(url, req.name, state_layers_output_path, overwrite_existing=overwrite_existing)


        for btype, data in self.additional_layers.items():
            for region_name, layers in data.items():
                nested_dirs = [btype, region_name]
                output_path = _DataFetcherHelper._nested_path(self.state_output_path, nested_dirs, make_if_absent=True)
                for layer_request in layers:
                    url = layer_request.prepare_url()
                    _DataFetcherHelper._fetch(url, layer_request.name, output_path, overwrite_existing=overwrite_existing)
        


class WADataFetcher(StateDataFetcher):
    def __init__(self):
        full_state_layer_requests = [
            _BoundaryRequest("county", "https://gis.dnr.wa.gov/site3/rest/services/Public_Boundaries/WADNR_PUBLIC_Cadastre_OpenData/FeatureServer/11/query", ["JURISDICT_SYST_ID", "JURISDICT_LABEL_NM"]),
            _BoundaryRequest("city", "https://services2.arcgis.com/J4VMdGWiZXReffvo/arcgis/rest/services/CityLimits/FeatureServer/0/query", ["OBJECTID", "CITY_NM", "COUNTY_NM"]),
            _BoundaryRequest("legislative_district", "https://services.arcgis.com/bCYnGqM4FMTBSjd1/arcgis/rest/services/Washington_State_Legislative_Districts_2022/FeatureServer/0/query", ["ID", "DISTRICT"]),
            _BoundaryRequest("congressional_district", "https://services.arcgis.com/bCYnGqM4FMTBSjd1/arcgis/rest/services/Washington_State_Congressional_Districts_2022/FeatureServer/0/query", ["ID", "DISTRICT"]),
            _BoundaryRequest("water_district", "https://services8.arcgis.com/rGGrs6HCnw87OFOT/arcgis/rest/services/Drinking_Water_Service_Areas/FeatureServer/0/query", ["OBJECTID", "WS_Name"]),
        ]
        additional_layers = {
            "city": {
                "seattle" : [_BoundaryRequest("city_council_district", "https://services.arcgis.com/ZOyb2t4B0UYuYNYH/arcgis/rest/services/Seattle_City_Council_Districts_2024/FeatureServer/0/query", ["C_DISTRICT", "DISPLAY_NAME"])]
            },
            "county": {
                "king" : [_BoundaryRequest("county_council_district", "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/district___base/MapServer/185/query", ["kccdst"])]
            }
        }
        super().__init__("WA", full_state_layer_requests, additional_layers)
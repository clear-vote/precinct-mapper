from __future__ import annotations
import requests
import json
import geopandas as gpd
from typing import List, Dict
from pathlib import Path
from urllib.parse import urlencode
from requests import Request, PreparedRequest, Session
import tempfile
import zipfile
import io
from typeguard import typechecked

@typechecked
class _DataFetcherHelper:
    datapath = Path(__file__).parent / "datasets"
    temppath = datapath / "temp"
    session = Session()

    @staticmethod
    def _fetch_from_geojson(request_builder: _BoundaryRequestBuilder, output_path: Path, overwrite_existing: bool = False):
        """Fetches the geoJSON from the given url and stores it as a geoJSON file
        under datasets/ using the given path extension.
        
        Args:
            url: web url to fetch data from.
            name: name stem of file to create (e.g., `\'city\'` for `\'city.geojson\'`).
            path: list of sequentially nested subdirectory names under `data/datasets/`
        
        Raises:
            RuntimeError: if issue accessing the given url for data.
        """
        output_path = output_path / f"{ request_builder.dst_name }.geojson"
        if overwrite_existing or not output_path.exists():
            data = _DataFetcherHelper._get_boundaries_from_geojson(request_builder)
            with open(output_path, "w") as file:
                file.write(data.to_json())

    @staticmethod
    def _fetch_from_shapefile(request_builder: _BoundaryRequestBuilder, output_path: Path, overwrite_existing: bool = False):
        """Fetches the shapefiles from the given url and stores them as geoJSON files
        under datasets/ using the given path extension.
        
        Args:
            url: web url to fetch data from.
            names: map of filenames from source to name stem of file to create
                (e.g., `\'city\'` for `\'city.geojson\'`).
            path: list of sequentially nested subdirectory names under `data/datasets/`
        
        Raises:
            RuntimeError: if issue accessing the given url for data.
        """
        data = _DataFetcherHelper._get_boundaries_from_shapefile(request_builder)
        file_output_path = output_path / f"{ request_builder.dst_name }.geojson"
        if overwrite_existing or not file_output_path.exists():
            with open(file_output_path, "w") as file:
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
    def _get_boundaries_from_geojson(request_builder: _BoundaryRequestBuilder, timeout: int = 10) -> gpd.GeoDataFrame:
        """Returns a GeoPandas GeoDataFrame of the location data found by
        querying the URL from the given request builder. Will give up after
        the given timeout in seconds.

        Args:
            request_builder: used to prepare the url to query.
            timeout: an integer number of seconds to wait for a response
                before quitting.
        
        Raises:
            RuntimeError: if could not connect to host with the given url or
                request timed out or another request error was faced
        """
        try:
            prepared_req = _DataFetcherHelper.session.prepare_request(request_builder.req)
            with _DataFetcherHelper.session.send(prepared_req, stream=True, timeout=timeout) as response:
                response.raise_for_status()
                raw_boundary_data = json.loads(response.content)
                processed_boundary_data = gpd.GeoDataFrame.from_features(raw_boundary_data["features"])
            return processed_boundary_data
        except requests.exceptions.ConnectionError as exc:
            raise RuntimeError("Could not connect.") from exc
        except requests.exceptions.Timeout as exc:
            raise RuntimeError("Request timed out.") from exc
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(f"An error occurred: {exc}") from exc
        
    @staticmethod
    def _get_boundaries_from_shapefile(request_builder: _BoundaryRequestBuilder, timeout: int = 10) -> gpd.GeoDataFrame:
        """Returns a GeoPandas GeoDataFrame of the location data found by
        querying the URL from the given request builder. Will give up after
        the given timeout in seconds.

        Args:
            request_builder: used to prepare the url to query.
            timeout: an integer number of seconds to wait for a response
                before quitting.
        
        Raises:
            RuntimeError: if could not connect to host with the given url or
                request timed out or another request error was faced
        """
        print("Fetching shapefile")
        try:
            prepared_req = _DataFetcherHelper.session.prepare_request(request_builder.req)
            with _DataFetcherHelper.session.send(prepared_req, stream=True, timeout=timeout) as response:
                response.raise_for_status()
                with tempfile.TemporaryDirectory(dir=_DataFetcherHelper.temppath) as tmpdir:
                    with zipfile.ZipFile(io.BytesIO(response.content), "r") as zip_ref:
                        zip_ref.extractall(tmpdir)
                    subdirectory = next(Path(tmpdir).iterdir())
                    processed_boundary_data = gpd.read_file(subdirectory / f"{ request_builder.src_name }.shp")
            # additional_columns = set(processed_boundary_data.columns) - set(request_builder.out_fields + ["geometry"])
            # processed_boundary_data.drop(additional_columns, axis=1, inplace=True)
            return processed_boundary_data[request_builder.out_fields + ["geometry"]]
        except requests.exceptions.ConnectionError as exc:
            raise RuntimeError("Could not connect.") from exc
        except requests.exceptions.Timeout as exc:
            raise RuntimeError("Request timed out.") from exc
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(f"An error occurred: {exc}") from exc
        
@typechecked
class _BoundaryRequestBuilder:
    """A class used to prepare urls for query from a GIS Database where the
    data is in GeoJSON or Shapefile format."""
    def __init__(self, dst_name: str, base_url: str, out_fields: List[str], src_format: str = "geojson", src_name: str | None = None):
        """Initializes a boundary request object.

        Args:
            base_url: url to query and (optionally) append parameters to (usually in case of geojson).
            format: Format of data to query (\'geojson\' or \'shapefile\').
            out_fields: for a geojson file, the list of fields to request. for all files, the column names to include.
            names: in case of shapefile request, names of shapefiles to extract from zip archive.

        Raises:
            ValueError if `format` is not \'geojson\' or \'shapefile\'
        """
        match src_format:
            case "geojson":
                self.req = Request(
                    method = "GET",
                    url = base_url,
                    params = {
                        "where": "1=1",
                        "outFields": ",".join(out_fields),
                        "f": "geojson"
                    }
                )
            case "shapefile":
                if src_name is None:
                    raise ValueError("Format \'shapefile\' requires argument \'src_name\'")
                self.req = Request(
                    method = "GET",
                    url = base_url
                )
            case _:
                raise ValueError(f"format must be one of (\'geojson\', \'shapefile\'). Got: \'{ src_format }\'")
        self.out_fields = out_fields
        self.dst_name = dst_name
        self.src_format = src_format
        self.src_name = src_name

    def prepare(self) -> PreparedRequest:
        """Returns the prepared request resulting from this builder."""
        return self.req.prepare()

@typechecked
class StateDataFetcher:
    def __init__(
            self,
            code,
            full_state_layer_requests: List[_BoundaryRequestBuilder],
            additional_layers: Dict[str, Dict[str, List[_BoundaryRequestBuilder]]]
        ):
        self.code = code
        self.state_output_path = _DataFetcherHelper.datapath / code
        self.state_output_path.mkdir(parents=True, exist_ok=True)
        self.full_state_layer_requests = full_state_layer_requests
        self.additional_layers = additional_layers

    def fetch(self, overwrite_existing):
        state_layers_output_path = self.state_output_path / "state"
        for req in self.full_state_layer_requests:
            match req.src_format:
                case "geojson":
                    _DataFetcherHelper._fetch_from_geojson(req, state_layers_output_path, overwrite_existing=overwrite_existing)
                case "shapefile":
                    _DataFetcherHelper._fetch_from_shapefile(req, state_layers_output_path, overwrite_existing=overwrite_existing)

        for btype, data in self.additional_layers.items():
            for region_name, layers in data.items():
                nested_dirs = [btype, region_name]
                output_path = _DataFetcherHelper._nested_path(self.state_output_path, nested_dirs, make_if_absent=True)
                for layer_request in layers:
                    match layer_request.src_format:
                        case "geojson":
                            _DataFetcherHelper._fetch_from_geojson(layer_request, output_path, overwrite_existing=overwrite_existing)
                        case "shapefile":
                            _DataFetcherHelper._fetch_from_shapefile(layer_request, output_path, overwrite_existing=overwrite_existing)
                    

@typechecked
class WADataFetcher(StateDataFetcher):
    def __init__(self):
        full_state_layer_requests = [
            _BoundaryRequestBuilder("precinct", "https://services.arcgis.com/jsIt88o09Q0r1j8h/arcgis/rest/services/Statewide_Precincts_2019General_SPS/FeatureServer/0/query", ["PrecCode", "CountyName"]),
            _BoundaryRequestBuilder("county", "https://gis.dnr.wa.gov/site3/rest/services/Public_Boundaries/WADNR_PUBLIC_Cadastre_OpenData/FeatureServer/11/query", ["JURISDICT_SYST_ID", "JURISDICT_LABEL_NM"]),
            _BoundaryRequestBuilder("city", "https://services2.arcgis.com/J4VMdGWiZXReffvo/arcgis/rest/services/CityLimits/FeatureServer/0/query", ["OBJECTID", "CITY_NM", "COUNTY_NM"]),
            _BoundaryRequestBuilder("legislative_district", "https://services.arcgis.com/bCYnGqM4FMTBSjd1/arcgis/rest/services/Washington_State_Legislative_Districts_2022/FeatureServer/0/query", ["ID", "DISTRICT"]),
            _BoundaryRequestBuilder("congressional_district", "https://services.arcgis.com/bCYnGqM4FMTBSjd1/arcgis/rest/services/Washington_State_Congressional_Districts_2022/FeatureServer/0/query", ["ID", "DISTRICT"]),
            _BoundaryRequestBuilder("water_district", "https://services8.arcgis.com/rGGrs6HCnw87OFOT/arcgis/rest/services/Drinking_Water_Service_Areas/FeatureServer/0/query", ["OBJECTID", "WS_Name"]),
        ]
        additional_layers = {
            "city": {
                "seattle" : [_BoundaryRequestBuilder("city_council_district", "https://services.arcgis.com/ZOyb2t4B0UYuYNYH/arcgis/rest/services/Seattle_City_Council_Districts_2024/FeatureServer/0/query", ["C_DISTRICT", "DISPLAY_NAME"])],
                "bellingham": [_BoundaryRequestBuilder("ward", "https://data.cob.org/data/gis/SHP_Files/COB_plan_shps.zip", ["GLOBALID", "WARD_NUMBE"], src_format="shapefile", src_name="COB_plan_Wards")]
            },
            "county": {
                "king" : [_BoundaryRequestBuilder("county_council_district", "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/district___base/MapServer/185/query", ["kccdst"])]
            }
        }
        super().__init__("WA", full_state_layer_requests, additional_layers)
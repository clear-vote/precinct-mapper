from __future__ import annotations

import io
import json
import tempfile
import zipfile
import pyproj
import pandas as pd
import geopandas as gpd

from typing import List, Dict
from pathlib import Path
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, RequestException
from typeguard import typechecked
from functools import partial
from shapely.ops import transform


@typechecked
class _DataFetcherHelper:
    datapath = Path(__file__).parent / "datasets"
    temppath = datapath / "temp"
    session = Session()

    @staticmethod
    def _fetch_from_geojson(
        request_builder: _BoundaryRequestBuilder,
        output_path: Path,
        overwrite_existing: bool = False,
    ):
        """Fetches the geoJSON from the givem request builder and stores it as a geoJSON file
        under datasets/ using the given path extension.

        Args:
            request_builder: contains data about the request to make and postprocessing.
            output_path: directory to write resulting geoJSON file to.
            overwrite_existing: if True, overwrites any existing files of the same name;
                if False, does not fetch remote data.

        Raises:
            RuntimeError: if issue accessing the given url for data.
        """
        output_path = output_path / f"{ request_builder.dst_name }.geojson"
        if overwrite_existing or not output_path.exists():
            data = _DataFetcherHelper._get_boundaries_from_geojson(request_builder)
            with open(output_path, "w") as file:
                file.write(data.to_json())

    @staticmethod
    def _fetch_from_shapefile(
        request_builder: _BoundaryRequestBuilder,
        output_path: Path,
        overwrite_existing: bool = False,
    ):
        """Fetches the shapefile from the givem request builder and stores it as a geoJSON file
        under datasets/ using the given path extension.

        Args:
            request_builder: contains data about the request to make and postprocessing.
            output_path: directory to write resulting geoJSON file to.
            overwrite_existing: if True, overwrites any existing files of the same name;
                if False, does not fetch remote data.

        Raises:
            RuntimeError: if issue accessing the given url for data.
        """
        data = _DataFetcherHelper._get_boundaries_from_shapefile(request_builder)
        file_output_path = output_path / f"{ request_builder.dst_name }.geojson"
        if overwrite_existing or not file_output_path.exists():
            with open(file_output_path, "w") as file:
                file.write(data.to_json())

    @staticmethod
    def _fetch_from_gdb(
        request_builder: _BoundaryRequestBuilder,
        output_path: Path,
        overwrite_existing: bool = False,
    ):
        data = _DataFetcherHelper._get_boundaries_from_gdb(request_builder)
        file_output_path = output_path / f"{ request_builder.dst_name }.geojson"
        if overwrite_existing or not file_output_path.exists():
            with open(file_output_path, "w") as file:
                file.write(data.to_json())


    @staticmethod
    def _nested_path(
        base: Path, subdirs: List[str], make_if_absent: bool = False
    ) -> Path:
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
            not_exists_message=f"Given base directory does not exist: {base.absolute()}",
        )
        output_path = base

        for subdir in subdirs:
            output_path /= subdir

            _DataFetcherHelper._not_exists_handler(
                output_path,
                make_if_absent=make_if_absent,
                not_exists_message=f"Child directory access failed. Directory does not exist: {output_path.absolute()}",
            )

        return output_path

    @staticmethod
    def _not_exists_handler(
        path: Path, make_if_absent: bool = False, not_exists_message: str = ""
    ):
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
    def _get_boundaries_from_geojson(
        request_builder: _BoundaryRequestBuilder, timeout: int = 10
    ) -> gpd.GeoDataFrame:
        """Returns a GeoPandas GeoDataFrame of the location data found by querying
        the URL for geoJSON data from the given request builder.
        Will give up after the given timeout in seconds.

        Args:
            request_builder: used to prepare the url to query.
            timeout: an integer number of seconds to wait for a response
                before quitting.

        Raises:
            RuntimeError: if could not connect to host with the given url or
                request timed out or another request error was faced

        Note that the queried data is processed using the information in request builder.
        Specifically:
            - only geometry column and columns in request_builder.out_fields.keys() are kept
            - columns are renamed according to request_builder.out_fields
            - all strings are converted to lowercase
        """
        try:
            request = Request(method="GET", url=request_builder.req.url, params=request_builder.req.params)
            result_offset = 0
            table_parts = []
            done_fetching = False
            while not done_fetching:
                request.params["resultOffset"] = result_offset
                prepared_req = _DataFetcherHelper.session.prepare_request(request)
                with _DataFetcherHelper.session.send(
                    prepared_req, stream=True, timeout=timeout
                ) as response:
                    response.raise_for_status()
                    raw_boundary_data = json.loads(response.content)
                    error = raw_boundary_data.get("error")
                    if error:
                        raise RuntimeError(f"Could not process query: {error}")
                    props = raw_boundary_data.get("properties")
                    if props:
                        exceeded = props.get("exceededTransferLimit")
                        if exceeded is None or exceeded == False:
                            done_fetching = True
                    else:
                        done_fetching = True

                    boundary_data = gpd.GeoDataFrame.from_features(
                        raw_boundary_data["features"]
                    )
                num_items = len(boundary_data)
                result_offset += num_items
                table_parts.append(boundary_data)
            
            full_table = pd.concat(table_parts, ignore_index=True)
            return _DataFetcherHelper._process_frame(
                full_table, request_builder.out_fields
            )
        except ConnectionError as exc:
            raise RuntimeError("Could not connect.") from exc
        except Timeout as exc:
            raise RuntimeError("Request timed out.") from exc
        except RequestException as exc:
            raise RuntimeError(f"An error occurred: {exc}") from exc

    @staticmethod
    def _get_boundaries_from_shapefile(
        request_builder: _BoundaryRequestBuilder, timeout: int = 10
    ) -> gpd.GeoDataFrame:
        """Returns a GeoPandas GeoDataFrame of the location data found by querying
        the URL for a zip archive of shapefile data from the given request builder.
        Will give up after the given timeout in seconds.

        Args:
            request_builder: used to prepare the url to query.
            timeout: an integer number of seconds to wait for a response
                before quitting.

        Raises:
            RuntimeError: if could not connect to host with the given url or
                request timed out or another request error was faced

        Note that the queried data is processed using the information in request builder.
        Specifically:
            - only geometry column and columns in request_builder.out_fields.keys() are kept
            - columns are renamed according to request_builder.out_fields
            - all strings are converted to lowercase
        """
        try:
            prepared_req = _DataFetcherHelper.session.prepare_request(
                request_builder.req
            )
            with _DataFetcherHelper.session.send(
                prepared_req, stream=True, timeout=timeout
            ) as response:
                response.raise_for_status()
                with tempfile.TemporaryDirectory(
                    dir=_DataFetcherHelper.temppath
                ) as tmpdir:
                    with zipfile.ZipFile(io.BytesIO(response.content), "r") as zip_ref:
                        zip_ref.extractall(tmpdir)
                    subdirectory = next(Path(tmpdir).iterdir())
                    boundary_data = gpd.read_file(
                        subdirectory / f"{ request_builder.src_name }.shp"
                    )
            return _DataFetcherHelper._process_frame(
                boundary_data, request_builder.out_fields
            )
        except ConnectionError as exc:
            raise RuntimeError("Could not connect.") from exc
        except Timeout as exc:
            raise RuntimeError("Request timed out.") from exc
        except RequestException as exc:
            raise RuntimeError(f"An error occurred: {exc}") from exc
        
    @staticmethod
    def _get_boundaries_from_gdb(request_builder: _BoundaryRequestBuilder, timeout: int = 10) -> gpd.GeoDataFrame:
        """Returns a GeoPandas GeoDataFrame of the location data found by querying
        the URL for a zip archive of shapefile data from the given request builder.
        Will give up after the given timeout in seconds.

        Args:
            request_builder: used to prepare the url to query.
            timeout: an integer number of seconds to wait for a response
                before quitting.

        Raises:
            RuntimeError: if could not connect to host with the given url or
                request timed out or another request error was faced

        Note that the queried data is processed using the information in request builder.
        Specifically:
            - only geometry column and columns in request_builder.out_fields.keys() are kept
            - columns are renamed according to request_builder.out_fields
            - all strings are converted to lowercase
        """
        try:
            prepared_req = _DataFetcherHelper.session.prepare_request(
                request_builder.req
            )
            with _DataFetcherHelper.session.send(
                prepared_req, stream=True, timeout=timeout
            ) as response:
                response.raise_for_status()
                with tempfile.TemporaryDirectory(
                    dir=_DataFetcherHelper.temppath
                ) as tmpdir:
                    with zipfile.ZipFile(io.BytesIO(response.content), "r") as zip_ref:
                        zip_ref.extractall(tmpdir)
                    subdirectory = next(Path(tmpdir).iterdir())
                    gdb = next(subdirectory.glob('*.gdb'))

                    boundary_data = gpd.read_file(
                        gdb,
                        layer=request_builder.src_name
                    )
            return _DataFetcherHelper._process_frame(
                boundary_data, request_builder.out_fields
            )
        except ConnectionError as exc:
            raise RuntimeError("Could not connect.") from exc
        except Timeout as exc:
            raise RuntimeError("Request timed out.") from exc
        except RequestException as exc:
            raise RuntimeError(f"An error occurred: {exc}") from exc

    def _process_frame(
        frame: gpd.GeoDataFrame, out_fields: Dict[str, str], lower: bool = True, project_to_84: bool = False
    ) -> gpd.GeoDataFrame:
        """Returns a GeoDataFrame with limited, renamed columnset and lowercase strings
        if specified.

        Args:
            frame: GeoDataFrame to process
            out_fields: maps source column names to output column names
            lower: if True, converts all strings to lowercase

        Note: if given frame has no crs, this function will set it to \'epsg:4326\' for WGS84
        """
        if frame.crs is None:
            frame.set_crs("epsg:4326")
        if project_to_84:
            frame.to_crs("epsg:4326", inplace=True)
        new_frame = frame[list(out_fields.keys()) + ["geometry"]]
        new_frame = new_frame.rename(out_fields, axis=1)
        if lower:
            for c in new_frame.select_dtypes("object").columns:
                new_frame[c] = new_frame[c].str.casefold()
        return new_frame


@typechecked
class _BoundaryRequestBuilder:
    """A class used to prepare urls for query from a GIS Database where the
    data is in GeoJSON or Shapefile format."""

    def __init__(
        self,
        dst_name: str,
        base_url: str,
        out_fields: Dict[str, str],
        src_format: str = "geojson",
        src_name: str | None = None,
    ):
        """Initializes a boundary request object.

        Args:
            dst_name: name stem of file to create (e.g., `\'city\'` for `\'city.geojson\'`).
            base_url: url to query and (optionally) append parameters to (in case of geojson).
            out_fields: maps the list of fields to request from source to output names to include
                in result.
            src_format: Format of data to query (\'geojson\' or \'shapefile\').
            src_name: in case of shapefile request, name of shapefile to extract from zip archive.

        Raises:
            ValueError if `format` is not \'geojson\' or \'shapefile\' or
                if \'src_name\' argument is not included when src_format is \'shapefile\'
        """
        match src_format:
            case "geojson":
                self.req = Request(
                    method="GET",
                    url = base_url,
                    params = {
                        "where": "1=1",
                        "outFields": ",".join(out_fields.keys()),
                        "geometryType": "esriGeometryPolygon",
                        "spatialRel": "esriSpatialRelIntersects",
                        "units": "esriSRUnit_NauticalMile",
                        "returnGeometry": "true",
                        "returnTrueCurves": "false",
                        "returnIdsOnly": "false",
                        "returnCountOnly": "false",
                        "returnZ": "false",
                        "returnM": "false",
                        "outSR": "{\"wkid\": 4326}",
                        "returnDistinctValues": "false",
                        "returnExtentOnly": "false",
                        "sqlFormat": "none",
                        "featureEncoding": "esriDefault",
		                "returnExceededLimitFeatures": "true",
                        "f": "geojson"
                    }
                )
            case "shapefile":
                if src_name is None:
                    raise ValueError("Format 'shapefile' requires argument 'src_name'")
                self.req = Request(method="GET", url=base_url)
            case "gdb":
                if src_name is None:
                    raise ValueError("Format 'gdb' requires argument 'src_name'")
                self.req = Request(method="GET", url=base_url)
            case _:
                raise ValueError(
                    f"format must be one of ('geojson', 'shapefile'). Got: '{ src_format }'"
                )
        self.out_fields = out_fields
        self.dst_name = dst_name
        self.src_format = src_format
        self.src_name = src_name


@typechecked
class StateDataFetcher:
    """A base class used to fetch geodata for a state"""

    def __init__(
        self,
        code: str,
        full_state_layer_requests: List[_BoundaryRequestBuilder],
        additional_layers: Dict[str, Dict[str, List[_BoundaryRequestBuilder]]],
    ):
        """Initializes this StateDataFetcher object.

        Args:
            code: the two-letter state code that corresponds to this fetcher.
            full_state_layer_requests: a list of boundary request builders for
                geodata available at the state level
            additional layers: a map of scope (e.g., \'city\', \'county\') to
                maps of region name to lists of boundary request builders
                specific to data for that region.
        
        Raises:
            ValueError if length of code is not 2.

        Note: will create a directory under datasets with the same name as the
        given code, if it does not already exist.
        """
        if len(code) != 2:
            raise ValueError(f"State codes must be two letters. Got: '{ code }'")
        self.code = code.upper()
        self.state_output_path = _DataFetcherHelper.datapath / code
        self.state_output_path.mkdir(parents=True, exist_ok=True)
        self.full_state_layer_requests = full_state_layer_requests
        self.additional_layers = additional_layers

    def fetch(self, overwrite_existing: bool):
        """Fetches all data for this state and writes to the state's directory under datasets.
        Output files are in geoJSON format

        Args:
            overwrite_existing: if True, overwrites any existing state files where there is a
                naming collision. Otherwise, does not fetch this data.
        """
        state_layers_output_path = self.state_output_path / "state"
        for req in self.full_state_layer_requests:
            try:
                match req.src_format:
                    case "geojson":
                        _DataFetcherHelper._fetch_from_geojson(
                            req,
                            state_layers_output_path,
                            overwrite_existing=overwrite_existing,
                        )
                    case "shapefile":
                        _DataFetcherHelper._fetch_from_shapefile(
                            req,
                            state_layers_output_path,
                            overwrite_existing=overwrite_existing,
                        )
                    case "gdb":
                        _DataFetcherHelper._fetch_from_gdb(
                            req,
                            state_layers_output_path,
                            overwrite_existing=overwrite_existing,
                        )
            except RuntimeError as e:
                print(f"Encountered error with { req.dst_name } at state level")
                print(e)

        for btype, data in self.additional_layers.items():
            for region_name, layers in data.items():
                nested_dirs = [btype, region_name]
                output_path = _DataFetcherHelper._nested_path(
                    self.state_output_path, nested_dirs, make_if_absent=True
                )
                for layer_request in layers:
                    try:
                        match layer_request.src_format:
                            case "geojson":
                                _DataFetcherHelper._fetch_from_geojson(
                                    layer_request,
                                    output_path,
                                    overwrite_existing=overwrite_existing,
                                )
                            case "shapefile":
                                _DataFetcherHelper._fetch_from_shapefile(
                                    layer_request,
                                    output_path,
                                    overwrite_existing=overwrite_existing,
                                )
                            case "gdb":
                                _DataFetcherHelper._fetch_from_gdb(
                                    layer_request,
                                    output_path,
                                    overwrite_existing=overwrite_existing,
                                )
                    except RuntimeError as e:
                        print(f"Encountered error with { layer_request.dst_name } for { region_name } at { btype } level")
                        print(e)


@typechecked
class WADataFetcher(StateDataFetcher):
    """Data Fetcher specific to Washington State."""

    def __init__(self):
        full_state_layer_requests = [
            _BoundaryRequestBuilder(
                "precinct",
                "https://services.arcgis.com/jsIt88o09Q0r1j8h/arcgis/rest/services/Statewide_Precincts_2019General_SPS/FeatureServer/0/query",
                {"OBJECTID": "id", "CountyName": "CountyName"},
            ),
            _BoundaryRequestBuilder(
                "county",
                "https://gis.dnr.wa.gov/site3/rest/services/Public_Boundaries/WADNR_PUBLIC_Cadastre_OpenData/FeatureServer/11/query",
                {"JURISDICT_SYST_ID": "id", "JURISDICT_LABEL_NM": "name"},
            ),
            _BoundaryRequestBuilder(
                "school_district",
                "https://services5.arcgis.com/q9Lwq3BC8p2H6RLg/arcgis/rest/services/Washington_School_District_Boundaries/FeatureServer/0/query",
                {"ESDNum": "id", "ShortName": "name"},
            ),
            _BoundaryRequestBuilder(
                "city",
                "https://services2.arcgis.com/J4VMdGWiZXReffvo/arcgis/rest/services/CityLimits/FeatureServer/0/query",
                {"OBJECTID": "id", "CITY_NM": "name", "COUNTY_NM": "COUNTY_NM"},
            ),
            _BoundaryRequestBuilder(
                "legislative_district",
                "https://services.arcgis.com/bCYnGqM4FMTBSjd1/arcgis/rest/services/Washington_State_Legislative_Districts_2022/FeatureServer/0/query",
                {"ID": "id", "DISTRICT": "name"},
            ),
            _BoundaryRequestBuilder(
                "congressional_district",
                "https://services.arcgis.com/bCYnGqM4FMTBSjd1/arcgis/rest/services/Washington_State_Congressional_Districts_2022/FeatureServer/0/query",
                {"ID": "id", "DISTRICT": "name"},
            )
        ]
        additional_layers = {
            "city": {
                "seattle": [
                    _BoundaryRequestBuilder(
                        "city_council_district",
                        "https://services.arcgis.com/ZOyb2t4B0UYuYNYH/arcgis/rest/services/Seattle_City_Council_Districts_2024/FeatureServer/0/query",
                        {"C_DISTRICT": "id", "DISPLAY_NAME": "name"},
                    ),
                    _BoundaryRequestBuilder(
                        "school_board_director_district",
                        "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/district___base/MapServer/406/query",
                        {"DIRDST": "id", "NAME": "name"}
                    )
                ],
                "bellingham": [
                    _BoundaryRequestBuilder(
                        "ward",
                        "https://data.cob.org/data/gis/FGDB_Files/COB_Planning.gdb.zip",
                        {"GLOBALID": "id", "WARD_NUMBER": "name"},
                        src_format="gdb",
                        src_name="plan_Wards",
                    )
                ]
            },
            "county": {
                "king": [
                    _BoundaryRequestBuilder(
                        "county_council_district",
                        "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/district___base/MapServer/185/query",
                        {"kccdst": "id"},
                    )
                ],
                "snohomish": [
                    _BoundaryRequestBuilder(
                        "county_court",
                        "https://services6.arcgis.com/z6WYi9VRHfgwgtyW/arcgis/rest/services/Court_Districts/FeatureServer/0/query",
                        {"District": "id"}
                    )
                ],
                "kitsap": [
                    _BoundaryRequestBuilder(
                        "port_district",
                        "https://services6.arcgis.com/qt3UCV9x5kB4CwRA/arcgis/rest/services/Port_District_Outlines/FeatureServer/0/query",
                        {"DISTRICT": "name"}
                    ),
                    _BoundaryRequestBuilder(
                        "county_comissioner_district",
                        "https://services6.arcgis.com/qt3UCV9x5kB4CwRA/arcgis/rest/services/County_Commissioner_District_Outlines/FeatureServer/0/query",
                        {"DISTRICT": "name"}
                    ),
                    _BoundaryRequestBuilder(
                        "port_comissioner_district",
                        "https://services6.arcgis.com/qt3UCV9x5kB4CwRA/arcgis/rest/services/Port_Commissioner_District_Outlines/FeatureServer/0/query",
                        {"DISTRICT": "name"}
                    ),
                ],
                "pierce": [
                    _BoundaryRequestBuilder(
                        "county_council_district",
                        "https://services2.arcgis.com/1UvBaQ5y1ubjUPmd/arcgis/rest/services/Pierce_County_Council_Districts/FeatureServer/0/query",
                        {"District_Number": "id", "Maplabel": "name", "council_homepage": "council_homepage"}
                    )
                ]
            },
        }
        super().__init__("WA", full_state_layer_requests, additional_layers)

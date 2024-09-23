from __future__ import annotations

import geopandas as gpd
import json
import pandas as pd
import re
import warnings
import zipfile

from io import BytesIO
from collections import defaultdict
from tempfile import TemporaryDirectory
from typing import List, Dict, Tuple
from pathlib import Path
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, RequestException
from typeguard import typechecked

@typechecked
def fetch_from_json(filepath: str | Path, output_dir: str | Path):
    """Provide a json file with configuration info used to fetch data and write to files."""
    # TODO:
    # - complete documentation
    # - clean up code (need helper functions lol)
    if isinstance(filepath, str):
        filepath = Path(filepath)
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    
    if not filepath.suffix == ".json":
        raise ValueError(f"Provided filepath { str(filepath) } is not a JSON file.")
    if not (filepath.exists() and filepath.is_file()):
        raise ValueError(f"Could not find file at requested filepath { str(filepath) }")
    
    with open(filepath, "rb") as fp:
        file_contents = json.load(fp)
        full_state_fetchers = {} # map of btype to fetcher
        additional_fetchers = defaultdict(lambda: defaultdict(lambda: defaultdict(str))) # map of scope to area name to btype to fetcher (lol)
    
        state_code = file_contents.get("state_code")
        if state_code is None:
            raise ValueError("JSON file does not have required field \"state_code\"")
        
        sources = file_contents.get("sources")
        if sources is None:
            raise ValueError("JSON file does not have required field \"sources\"")
        
        for scope, area_entries in sources.items():
            for area_name, entries in area_entries.items():
                for entry in entries:
                    src_format = entry.get("source_format")
                    cur_fetcher = None
                    url = entry.get("url")
                    btype = entry.get("boundary_type")
                    if btype is None:
                        raise ValueError(f"An entry for {scope} {area_name} is missing boundary_type")
                    if url is None:
                        raise ValueError(f"An entry for {scope} {area_name} is missing url")
                    field_mappings = entry.get("field_mappings")
                    src_to_dst_fields = {src: dst_entry.get("dst_field") for src, dst_entry in field_mappings.items() }
                    src_to_dst_regex = {}
                    for src, dst_entry in field_mappings.items():
                        regex = dst_entry.get("regex")
                        if regex is not None:
                            src_to_dst_regex[src] = regex
                    match src_format:
                        case "arcgis_geojson":
                            cur_fetcher = GeoJSONFetcher(
                                url,
                                src_to_dst_fields,
                                src_to_dst_regex,
                                True
                            )
                        case "geojson":
                            cur_fetcher = GeoJSONFetcher(
                                url,
                                src_to_dst_fields,
                                src_to_dst_regex,
                                False
                            )
                        case "gdb":
                            cur_fetcher = GDBFetcher(
                                url,
                                src_to_dst_fields,
                                src_to_dst_regex,
                                entry.get("folder_name"),
                                entry.get("layer_name")
                            )
                        case _:
                            raise ValueError(f"Source format { src_format } is not allowed")
                    print(scope, area_name, btype, url, src_to_dst_fields)
                    if scope == "state":
                        full_state_fetchers[btype] = cur_fetcher
                    else:
                        additional_fetchers[scope][area_name][btype] = cur_fetcher
    
    # for state_btype, state_fetcher in full_state_fetchers.items():
    #     print(f"Fetching state data for {state_btype}")
    #     data = state_fetcher.fetch()
    #     GeoWriter.write(data, output_dir / "state", )
    state_data_fetcher = StateDataFetcher(
        state_code,
        full_state_fetchers,
        additional_fetchers,
        output_dir
    )
    state_data_fetcher.fetch()

@typechecked
class _GeoFetcherBase:
    """A base class to fetch geodata from a single data source.
    
    Extending classes should override the function fetch_unchecked.
    """

    def __init__(self,
                 url: str,
                 src_to_dst_fields: Dict[str, str],
                 src_to_dst_regex: Dict[str, str],
                 parameters: Dict[str, str] = {}):
        """Initializes this fetcher.
        
        :param url: the web url to fetch data from.
        :type url: str
        :param src_to_dst_fields: a dictionary mapping columns/attributes names
            in the source data to their names in the output data.
        :type src_to_dst_fields: dict
        :param src_to_dst_regex: a dictionary mapping columns/attributes names
            in the source data to a regex string to process values in that column.
            the regex should have a named group with a name that matches the dst
            entry in `src_to_dst_fields`
        :type src_to_dst_regex: dict
        :param parameters: parameters to include in the request on top of the
            provided URL.
        :type parameters: dict, optional
        """
        self.req = Request("GET", url, params=parameters)
        self.session = Session()
        self.src_to_dst_fields = src_to_dst_fields
        self.src_to_dst_regex = src_to_dst_regex

    def fetch(self, timeout: int = 10) -> gpd.GeoDataFrame:
        """Fetches geodata from the source provided on initialization and
            returns it as a GeoPandas dataframe.
            
        :param timeout: the time, in seconds, to wait for a response from the source.
        :type timeout: int, optional

        Note: this calls the fetch_unchecked function; make sure to override this.
        """
        try:
            return self.fetch_unchecked(timeout)
        except ConnectionError as exc:
            raise RuntimeError("Could not connect.") from exc
        except Timeout as exc:
            raise RuntimeError("Request timed out.") from exc
        except RequestException as exc:
            raise RuntimeError(f"An error occurred: {exc}") from exc

    def fetch_unchecked(self, timeout: int = 10) -> gpd.GeoDataFrame:
        """Fetch
        """
        warnings.warn("fetch_unchecked should be overriden by child classes.")

    @staticmethod
    def _process_column(initial_value, regex, dst_name):
        match = re.search(regex, initial_value)
        if match:
            val = match.group(dst_name)
            if val is None:
                raise ValueError(f"Group { dst_name } could not be found in input, { initial_value }")
            print(f"Input: { initial_value }, Output: { val }")
            return val
        else:
            raise ValueError(f"Could not find matching pattern for \"{ regex }\" in input, \"{ initial_value }\".")

    def _process_frame(
        self, frame: gpd.GeoDataFrame, lower: bool = True
    ) -> gpd.GeoDataFrame:
        """Returns a GeoDataFrame with limited, renamed columnset and lowercase strings
        if specified.

        Args:
            frame: GeoDataFrame to process
            lower: if True, converts all strings to lowercase

        Note: if given frame has no crs, this function will set it to \'epsg:4326\' for WGS84
        """
        for src_field in self.src_to_dst_fields:
            if src_field not in frame.columns:
                raise ValueError(f"Given dataframe has invalid source column: {src_field}. Must be one of {list(self.src_to_dst_fields.keys())}")
        frame.to_crs("epsg:4326", inplace=True)
        new_frame = frame[list(self.src_to_dst_fields.keys()) + ["geometry"]]
        for src, regex in self.src_to_dst_regex.items():
            dst_name = self.src_to_dst_fields[src]
            new_frame[src] = new_frame[src].apply(
                lambda initial_value: _GeoFetcherBase._process_column(initial_value, regex, dst_name)
            )

        new_frame = new_frame.rename(self.src_to_dst_fields, axis=1)
        
        if lower:
            for c in new_frame.select_dtypes("object").columns:
                new_frame[c] = new_frame[c].str.casefold()
        return new_frame
    
    def __str__(self):
        return f"Fetcher({ self.req.url }, { self.src_to_dst_fields })"

@typechecked
class GeoWriter:
    @staticmethod
    def output_path(output_dir: Path, name: str, fileformat: str = "pickle"):
        if fileformat == "pickle":
            return output_dir / f"{name}.pkl"
        return output_dir / f"{name}.{fileformat}"

    @staticmethod
    def write(boundaries: gpd.GeoDataFrame, output_dir: Path, output_filename: str, name: str):
        output_path = output_dir / f"{output_filename}.gpkg"
        GeoWriter._handle_not_exists(output_dir, make_if_absent=True)
        boundaries.to_file(output_path, layer=name, driver="GPKG")

    @staticmethod
    def nested_path(base: Path, subdirs: List[str], make_if_absent: bool = False) -> Path:
        """Returns the path found by accessing nested child subdirectories from the base.

        For example, given base: \'~/\' and subdirs [\'Users\', \'Voter\'], the Path of the
        directory corresponding to `~/Users/Voter/` would be returned.

        Args:
            base: path to base directory
            subdirs: list of names of nested child subdirectories

        Raises:
            FileNotFoundError: if base directory or any child directory does not exist.
        """
        GeoWriter._handle_not_exists(
            base,
            make_if_absent=make_if_absent,
            not_exists_message=f"Given base directory does not exist: {base.absolute()}",
        )
        output_path = base

        for subdir in subdirs:
            output_path /= subdir

            GeoWriter._handle_not_exists(
                output_path,
                make_if_absent=make_if_absent,
                not_exists_message=f"Child directory access failed. Directory does not exist: {output_path.absolute()}",
            )

        return output_path

    @staticmethod
    def _handle_not_exists(
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
        # if not path.is_dir():
        #     raise ValueError(f"Path must be a directory. Got {path}")
        if not path.exists():
            if make_if_absent:
                path.mkdir(parents=True)
            else:
                raise FileNotFoundError(not_exists_message)

@typechecked
class GeoJSONFetcher(_GeoFetcherBase):
    def __init__(self, url: str, src_to_dst_fields: Dict[str, str], src_to_dst_regex: Dict[str, str], from_arcgis: bool = True):
        if from_arcgis: \
            params = {
                "where": "1=1",
                "outFields": ",".join(src_to_dst_fields.keys()),
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
                "resultOffsrt": 0,
                "featureEncoding": "esriDefault",
                "returnExceededLimitFeatures": "true",
                "f": "geojson"
            }
        else:
            params = {}

        super().__init__(
            url,
            src_to_dst_fields,
            src_to_dst_regex,
            params
        )

    def fetch_unchecked(self, timeout: int = 10) -> gpd.GeoDataFrame:
        result_offset = 0
        table_parts = []
        done_fetching = False
        while not done_fetching:
            self.req.params["resultOffset"] = result_offset
            prepared_req = self.session.prepare_request(self.req)
            
            with self.session.send(
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
                    print(exceeded)
                    if exceeded is None or exceeded == False:
                        done_fetching = True
                else:
                    done_fetching = True

                boundary_data = gpd.GeoDataFrame.from_features(
                    raw_boundary_data["features"],
                    crs="EPSG:4326"
                )
            num_items = len(boundary_data)
            result_offset += num_items
            table_parts.append(boundary_data)

        full_table = pd.concat(table_parts, ignore_index=True)
        self.req.params["resultOffset"] = 0
        return self._process_frame(full_table)
    
@typechecked
class ShapefileFetcher(_GeoFetcherBase):
    def __init__(self, url: str, src_to_dst_fields: Dict[str, str], src_to_dst_regex: Dict[str, str], src_name: str):
        super().__init__(url, src_to_dst_fields, src_to_dst_regex)
        self.src_name = src_name

    def fetch_unchecked(self, dirs: str | List[str], timeout: int = 10) -> gpd.GeoDataFrame:
        if isinstance(dirs, str):
            pass

        prepared_req = self.session.prepare_request(
            self.req
        )
        with self.session.send(
            prepared_req, stream=True, timeout=timeout
        ) as response:
            response.raise_for_status()
            with TemporaryDirectory() as tempdir:
                with zipfile.ZipFile(BytesIO(response.content), "r") as zip_ref:
                    zip_ref.extractall(tempdir)
                    shps = tempdir.rglob(f"{self.src_name}*.shp")
                    first_shp_path = next(shps, None)
                    if first_shp_path is None:
                        raise RuntimeError(f"No shapefile with name {self.src_name} found in requested directory")
                    
                    boundary_data = gpd.read_file(first_shp_path)

        return self._process_frame(boundary_data)

@typechecked
class GDBFetcher(_GeoFetcherBase):
    def __init__(self, url: str, src_to_dst_fields: Dict[str, str], src_to_dst_regex: Dict[str, str], src_name: str, layer_name: str):
        super().__init__(url, src_to_dst_fields, src_to_dst_regex)
        self.src_name = src_name
        self.layer_name = layer_name
    
    def fetch_unchecked(self, timeout: int = 10) -> gpd.GeoDataFrame:
        prepared_req = self.session.prepare_request(
            self.req
        )
        with self.session.send(
            prepared_req, stream=True, timeout=timeout
        ) as response:
            response.raise_for_status()
            with TemporaryDirectory() as tempdir:
                with zipfile.ZipFile(BytesIO(response.content), "r") as zip_ref:
                    zip_ref.extractall(tempdir)
                
                gdbs = Path(tempdir).rglob(f"*{self.src_name}.gdb")
                # print([p for p in Path(tempdir).rglob("*")])
                first_gdb_path = next(gdbs, None)
                if first_gdb_path is None:
                    raise RuntimeError(f"No gdb with name {self.src_name} found in requested directory")
                
                boundary_data = gpd.read_file(first_gdb_path, layer=self.layer_name)

        return self._process_frame(boundary_data)


@typechecked
class StateDataFetcher:
    """A base class used to fetch geodata for a state"""

    def __init__(
        self,
        code: str,
        full_state_fetchers: Dict[str, _GeoFetcherBase],
        additional_fetchers: Dict[str, Dict[str, Dict[str, _GeoFetcherBase]]],
        output_dir: Path = Path(__file__).parent / "datasets"
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
        # TODO: update documentation
        if len(code) != 2:
            raise ValueError(f"State codes must be two letters. Got: '{ code }'")
        self.code = code.upper()
        self.state_output_path = output_dir / code
        self.state_output_path.mkdir(parents=True, exist_ok=True)
        self.full_state_fetchers = full_state_fetchers
        self.additional_fetchers = additional_fetchers

    def fetch(self):
        """Fetches all data for this state and writes to the state's directory under datasets.
        Output files are in geoJSON format

        Args:
            overwrite_existing: if True, overwrites any existing state files where there is a
                naming collision. Otherwise, does not fetch this data.
        """
        state_layers_output_path = self.state_output_path / "state"
        for name, fetcher in self.full_state_fetchers.items():
            print(name)
            geodata = fetcher.fetch()
            GeoWriter.write(geodata, state_layers_output_path, name, name)
            # try:                
            # except RuntimeError as e:
            #     warnings.warn(
            #         f"Encountered error with { name } at state level",
            #         source = e
            #     )

        for btype, data in self.additional_fetchers.items():
            for region_name, layers in data.items():
                nested_dirs = [btype, region_name]
                output_dir = GeoWriter.nested_path(self.state_output_path, nested_dirs, make_if_absent=True)
                for name, layer_fetcher in layers.items():
                    print(btype, region_name, name)
                    geodata = layer_fetcher.fetch()
                    GeoWriter.write(geodata, output_dir, name, name)
                    # try:
                    # except RuntimeError as e:
                    #     warnings.warn(
                    #         f"Encountered error with { name } at state level",
                    #         source = e
                    #     )


@typechecked
class WADataFetcher(StateDataFetcher):
    """Data Fetcher specific to Washington State."""

    def __init__(self, full_state_fetchers, additional_fetchers):
        super().__init__("WA", full_state_fetchers, additional_fetchers)

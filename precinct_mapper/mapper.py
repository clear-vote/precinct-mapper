from __future__ import annotations
from pathlib import Path
from precinct_mapper.data.containers import State
from precinct_mapper.data.parser import StateParser
from typeguard import typechecked

_datapath = Path(__file__).parent / "data" / "datasets"

@typechecked
def load_state(code: str = "WA", from_cache: bool = True) -> State:
    if from_cache:
        cache_filepath = _datapath / code / f"{code}_Cache.pkl"
        return State.from_cache(cache_filepath)
    parser = StateParser(code)
    return parser.parse()

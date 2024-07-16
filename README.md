# precinct-mapper
A Python Package to preprocess voting precinct and district geodata and make it easy to query. Currently restricted to WA state.

# To run in DEVELOPMENT
1. make sure you have Jupyter and ipykernel installed
2. create a conda environment (-f flag specifies file): `conda env create -f dev_env.yaml`. This will create a conda environment called 'precinct_mapper'
3. activate that environment: `conda activate precinct_mapper`

# To run in PRODUCTION
1. create a virtual environment `python -m venv myenv`
2. then activate it `source myenv/bin/activate`
2. pip install precinct_mapper `pip install precinct_mapper`
TODO: Anaya install requests!!! (pip install requests)
3. issue the following commands...
```
from precinct_mapper.mapper import load_state
state_obj = load_state()
json = state_obj.lookup_lat_lon(-122.3328, 47.6061)
print(json['county'].name)

```
u gud to go
# precinct-mapper
A Python Package to preprocess voting precinct and district geodata and make it easy to query. Currently restricted to WA state.

# To run in DEVELOPMENT
1. make sure you have Jupyter and ipykernel installed
2. create a conda environment (-f flag specifies file): `conda env create -f dev_env.yaml`. This will create a conda environment called 'precinct_mapper'
3. activate that environment: `conda activate precinct_mapper`

# To run in PRODUCTION
1. create a virtual environment `python -m venv venv`
2. then activate it `source venv/bin/activate`
3. pip install precinct_mapper `pip install git+https://github.com/clear-vote/precinct-mapper.git`
4. TODO: Anaya install requests!!! `pip install requests`
5. Save it all to requirements.txt `pip freeze > requirements.txt` *in the virtual environment*
6. issue the following commands...
```
from precinct_mapper.mapper import load_state
state_obj = load_state()
json = state_obj.lookup_lat_lon(-122.3328, 47.6061)
print(json['county'].name)

```
u gud to go

## SSH Config (so you can modify AWS code locally)
Insert the following ssh code
1. Ask for the key (keep it somewhere safe)
2. Add the following SSH info to your config
> Host flask-app-ec2
>     HostName ec2-35-88-126-46.us-west-2.compute.amazonaws.com
>     User ubuntu
>     IdentityFile <your-key-name-here>

## (we are moving away from this) Restarting for production on AWS EC2 instance
1. Restart and enable the flaskapp: `sudo systemctl restart flaskapp && sudo systemctl enable flaskapp`
2. Check the localhost `curl -v http://localhost:8000`
3. Check the IP `curl http://35.88.126.46/?longitude=0&latitude=0`... *Always append http:// before any IP!*
4. (optional) If you ever change the public IP, make sure to update it in the flaskapp configuration at /etc/nginx/sites-available/flaskapp
5. (optional) reload the configurations and restart nginx. You can also modify gunicorn /etc/systemd/system/flaskapp.service

## Lambda
1. Run the following
cp lambda_function.py my-deployment-package/
cp -r venv/lib/python3.x/site-packages/* my-deployment-package/
2. Push to GH
3. Download zip from GH
4. Deploy via AWS Lambda

## TODO
Amazon RDS (Relational Database Service): To host your MySQL database.
Amazon S3: To store static assets or backup data if needed.
AWS IAM (Identity and Access Management): To manage access and permissions securely.

Add this route and test with different coordinates:
```python
from flask import Flask, request, jsonify
from your_module import state_obj  # Import your state_obj from the appropriate module

app = Flask(__name__)

@app.route('/lookup', methods=['GET'])
def lookup_lat_lon():
    # Get coordinates from query parameters
    coordinate_1 = request.args.get('coordinate_1')
    coordinate_2 = request.args.get('coordinate_2')
    
    if not coordinate_1 or not coordinate_2:
        return jsonify({'error': 'Missing coordinates'}), 400
    
    try:
        # Run the command with the provided coordinates
        result = state_obj.lookup_lat_lon(coordinate_1, coordinate_2)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
```

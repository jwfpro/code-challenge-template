# Corteva Code Challenge: Weather and Grain Yield APIs

Get Started

Requirements:
python3, pip3, postgres


The code can be found in the answers directory. It is split up into two components: 
1. Ingestion (ingestion.py):
	Converts the csv files to Pandas DataFrames
	Concatenates the DataFrames and adds headers
	Creates any new tables in Postgres DB
	Converts DataFrames into new csv files (in memory)
	Writes csv files to Postgres DB
2. API Server (app.py)
	Launches Flask server
	Routes for weather, yield, or stats data
	Queries Postgres DB with query params
	Paginates data
	Returns JSON response

There are 4 other helper files:
1) utils.py: generalizable helper functions
2) constants.py: constants used by the program
3) db_config.py: Postgres connection info
4) unit_tests.py: Some simple unit tests

```
pip3 install -r requirements.txt
```

To ingest data into your postgres server:

Insert your db credentials into db_config.py

python3 ingestion.py

Once data is successfully inserted (logged around 16000 ms on my machine) start the flask server:

python3 app.py

It should be running on the default http://127.0.0.1:5000, as displayed in the terminal window.

Use Postman or the browser of your choice to send requests to the 3 API endpoints:

/api/weather [filters: date, station_id]
/api/yield [filters: year]
/api/weater/stats [filters: year, station_id]

For each there will be a JSON response that can be filtered by query params, shown respectively for each endpoint above. If an offset and limit are not specified as query params, the defaults in the app.py file will be used.



Additional Notes:

1) I chose to go with a Pandas DataFrame solution for manipulating the csv data before insert, although the vast majority of the runtime stems from Postgres' native "COPY" solution for reading csv files (which was significantly faster than Pandas' equivalent to_sql). While a faster implementation may be built leveraging PySpark for parallel processing, this solution would still be based on the "COPY" command utility.

2) I wasn't satisfied with standard Flask pagination solutions (requiring SQLAlchemy and ORM or not easily compatible with JSON responses) so I decided to use a custom implementation. The offset and limit are optional arguments with default values. These values are used to build out the URL for the previous and next entries, where applicable.

3) Thank you for checking out my code! Please reach out if there are any questions.
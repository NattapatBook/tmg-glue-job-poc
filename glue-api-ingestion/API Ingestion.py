import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# set custom logging on
logger = glueContext.get_logger()
#write into the log file with:
# logger.info("-----------------------------> HELLO WORLD")


# Setup the Open-Meteo API client with cache and retry on error
# cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
# retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
# openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
# url = "https://api.open-meteo.com/v1/forecast"
# params = {
# 	"latitude": 13.7724,
# 	"longitude": 100.5355,
# 	"hourly": "temperature_2m",
# 	"timezone": "Asia/Bangkok"
# }
# responses = openmeteo.weather_api(url, params=params)

# response = responses[0]
# print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
# print(f"Elevation {response.Elevation()} m asl")
# print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
# print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# # Process hourly data. The order of variables needs to be the same as requested.
# hourly = response.Hourly()
# hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

# hourly_data = {"date": pd.date_range(
# 	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
# 	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
# 	freq = pd.Timedelta(seconds = hourly.Interval()),
# 	inclusive = "left"
# )}

# hourly_data["temperature_2m"] = hourly_temperature_2m

# hourly_dataframe = pd.DataFrame(data = hourly_data)
# print(hourly_dataframe)


# Create glue dynamic frame
# sp_hourly_dataframe = spark.createDataFrame(hourly_dataframe)
# dy_hourly_dataframe = DynamicFrame.fromDF(sp_hourly_dataframe, glueContext, "dy_hourly_dataframe")

# wheather_forecast = glueContext.write_dynamic_frame.from_options(frame=dy_hourly_dataframe, connection_type="s3", format="csv", connection_options={"path": "s3://tmg-s3-raw-poc/meteo_json/", "partitionKeys": []}, transformation_ctx="wheather_forecast")



import requests
import json

url = "https://api.open-meteo.com/v1/forecast?latitude=15.5&longitude=101&hourly=temperature_2m&timezone=Asia%2FBangkok&forecast_days=1"
responses = requests.get(url)
# logger.info(responses.text)
res = json.loads(responses.text)

hourly = res['hourly']
hourly_data = pd.DataFrame({"date": hourly['time']})
hourly_data["temperature_2m"] = hourly['temperature_2m']

# Create glue dynamic frame
sp_hourly_dataframe = spark.createDataFrame(hourly_data)
dy_hourly_dataframe = DynamicFrame.fromDF(sp_hourly_dataframe, glueContext, "dy_hourly_dataframe")

wheather_forecast = glueContext.write_dynamic_frame.from_options(frame=dy_hourly_dataframe, connection_type="s3", format="csv", connection_options={"path": "s3://tmg-s3-raw-poc/meteo_json/", "partitionKeys": []}, transformation_ctx="wheather_forecast")

job.commit()
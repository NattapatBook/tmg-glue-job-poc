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

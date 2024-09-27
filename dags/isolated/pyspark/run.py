from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, TimestampType
from pyspark.sql.functions import col, lit, expr, current_timestamp, date_format
import json

spark = SparkSession.builder \
	.appName("PySparkDataTransform") \
	.config('spark.sql.parquet.datetimeRebaseModelInRead', 'CORRECTED') \
	.config('spark.sql.session.timeZone', 'UTC') \
	.getOrCreate()

current_timezone = spark.conf.get('spark.sql.session.timeZone')
print(f'Current Timezone: {current_timezone}')

with open('test.json', 'r') as f:
	print(json.loads(f.read()))

spark.stop()
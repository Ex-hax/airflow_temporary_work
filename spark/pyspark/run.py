from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import *
# from pyspark.sql.types import *
from pyspark.sql.types import StructType, TimestampType
import json
import argparse
import os

@udf(TimestampType())
def extract_member0(value):
    if value is not None:
        if hasattr(value, 'member0'):
            value = value.member0
        else:
            # For the struct type which don't have member0 attr AS IS NOT FOUND THIS CONDITION
            value = json.dumps(value.asDict(), ensure_ascii=False)
    else:
        value = None
    return value

def main():
    spark = SparkSession.builder \
        .appName("spark_test") \
        .config("spark.sql.streaming.schemaInference", True) \
        .config("spark.rpc.message.maxSize","1024")  \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    current_timezone = spark.conf.get("spark.sql.session.timeZone")
    print(f"Current TimeZone: {current_timezone}")

    # with open('/opt/bitnami/spark/work/test_ecommerce/config/spw_products_agg.json') as file:
    #     spark_json = json.load(file)['spark_json']
    # print(spark_json)

    print(" >>>>>>>>>>>>>>>>>> Spark session about to stop.")
    spark.stop()

if __name__ == "__main__":
    main()
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import *

# Create the spark session

spark = SparkSession.builder.master("local") \
    .appName("count-os-usage") \
    .getOrCreate()

website_logs = spark.read.json("hdfs://0.0.0.0:9075/user/gigahex/logs_devices.json")
with_hour = website_logs.withColumn("hour", hour(to_timestamp(col("timestamp"))))
with_hour.show()
total_users = website_logs.count()
stats = with_hour.withColumn("range", concat(col("hour"), lit(" to "), col("hour") + 1)) \
    .groupBy("range") \
    .count() \
    .rdd \
    .map(lambda row: Row(time_range=row[0],
                         users=row[1],
                         percentage_users=(row[1] / total_users) * 100)).toDF().sort(desc("users"))

stats.show()

spark.stop()

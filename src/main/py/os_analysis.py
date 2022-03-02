from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import desc
from ua_parser import user_agent_parser

# Create the spark session

spark = SparkSession.builder.master("local") \
    .appName("count-os-usage") \
    .getOrCreate()

website_logs = spark.read.json("hdfs://0.0.0.0:9075/user/gigahex/logs_devices.json")
with_os = website_logs \
    .rdd \
    .map(lambda row: Row(
    user_id=row.asDict()["user_id"],
    os=user_agent_parser.ParseOS(row.asDict()["user_agent"])['family'],
)).toDF()

with_os.show()

total_users = website_logs.count()
stats = with_os.groupBy("os") \
    .count() \
    .rdd \
    .map(lambda row: Row(operating_system=row[0],
                         users=row[1],
                         percentage_users=(row[1] / total_users) * 100)).toDF().sort(desc("users"))
stats.show()

spark.stop()

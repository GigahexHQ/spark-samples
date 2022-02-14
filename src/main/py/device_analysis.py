from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import desc
from ua_parser import user_agent_parser

# Create the spark session

spark = SparkSession.builder.master("local") \
    .appName("count-browser-usage") \
    .getOrCreate()

website_logs = spark.read.json("/Users/shad/logs_devices.json")
with_browser = website_logs \
    .rdd \
    .map(lambda row: Row(
    user_id=row.asDict()["user_id"],
    browser=user_agent_parser.ParseUserAgent(row.asDict()["user_agent"])['family'],
)).toDF()

with_browser.show()

total_users = website_logs.count()
stats = with_browser.groupBy("browser") \
    .count() \
    .rdd \
    .map(lambda row: Row(browser=row[0],
                         users=row[1],
                         percentage_users=(row[1] / total_users) * 100)).toDF().sort(desc("users"))
stats.show()

spark.stop()

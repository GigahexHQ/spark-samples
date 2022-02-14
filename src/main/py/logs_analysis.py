from pyspark.sql import SparkSession
from pyspark.sql.types import Row

# Create the spark session

spark = SparkSession.builder.master("local")\
    .appName("count-words-python")\
    .getOrCreate()
   
ip_database = {}
with open("/Users/shad/ip.db") as f:
    table = f.read().split("\n")
    for row in table:
        attr = row.split(",")
        if len(attr) > 1:
            ip_database[attr[0]] = attr[1]
        

# print(get_country_http("122.180.169.236"))

website_logs = spark.read.json("/Users/shad/logs.json")
with_country = website_logs.rdd.map(lambda row: Row(ip_addr = row.asDict()["ip_addr"], user_id = row.asDict()["user_id"], country=ip_database[row.asDict()["ip_addr"]])).toDF()
total_users = with_country.count()
stats = with_country.groupBy("country").count().rdd.map(lambda row: Row(country = row[0], users = row[1], percentage_users = (row[1]/total_users)*100)).toDF()
stats.show()

spark.stop()
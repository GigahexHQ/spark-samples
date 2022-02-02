from pyspark.sql import SparkSession
from pyspark.sql.types import Row

# Create the spark session
spark = SparkSession.builder.master("local").appName("count-words-python").getOrCreate()
text = spark.read.text("/path/to/hello.in")
words = text.rdd.flatMap(lambda line: line.value.split(" "))
large_words_df = words.filter(lambda w: len(w) > 2).map(lambda line: Row(line)).toDF()
large_words_df.write.text("/path/to/large-out-py")
spark.stop()

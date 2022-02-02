package com.gigahex.samples

import org.apache.spark.sql.SparkSession

object HelloWord {

  def main(args: Array[String]): Unit = {
    /**
     * Setup the spark session
     */
    val spark = SparkSession.builder()
      .master("local")
      .appName("count-words-scala")
      .getOrCreate()
    import spark.implicits._

    val text = spark.read.textFile("")
    val words = text.flatMap(x => x.split(" "))
    val largeWords = words.filter(w => w.length > 2)
    largeWords.write.text("/path/to/large-words")

    spark.stop()
  }

}

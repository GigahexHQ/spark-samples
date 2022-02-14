package com.gigahex.samples

import java.lang.System.Logger

import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.SparkSession

object HelloWord {

  val logger = LogFactory.getLog(getClass)

  def main(args: Array[String]): Unit = {
    /**
     * Setup the spark session
     */
    val spark = SparkSession.builder()
      .appName("count-words-scala")
      .getOrCreate()
    import spark.implicits._

    val text = spark.read.textFile("/Users/gigahex/hello.in")
    val words = text.flatMap(x => x.split(" "))
    val largeWords = words.filter(w => w.length > 2)
    val outputPath = s"/Users/gigahex/large-words-scala/${System.currentTimeMillis()}"
    largeWords.write.text(outputPath)
    spark.read.textFile(s"${outputPath}/*").show(30)

    spark.stop()
  }

}

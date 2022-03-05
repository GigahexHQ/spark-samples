package com.gigahex.samples.analytics

import nl.basjes.parse.useragent.UserAgentAnalyzer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object TimeAnalysis {


  def main(args: Array[String]): Unit = {
    /**
     * Setup the spark session
     */
    val spark = SparkSession.builder()
      .appName("devices_usage")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val websiteLogs = spark.read.json("hdfs://0.0.0.0:9075/user/gigahex/logs_devices.json")
    val withHour = websiteLogs.withColumn("hour", hour(from_unixtime(col("timestamp")))).cache()
    withHour.show()
    val total = withHour.count()
    withHour.withColumn("range", concat($"hour", lit(" to "), $"hour" + 1))
      .groupBy($"range")
      .count()
      .map(row => (row.getAs[String]("range"),
        row.getAs[Long]("count"),
        (row.getAs[Long]("count") / total.toDouble) * 100))
      .toDF("Time of Day", "Users", "% Users")
      .orderBy(desc("Users"))
      .show()

    spark.stop()


  }

}

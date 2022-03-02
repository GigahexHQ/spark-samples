package com.gigahex.samples

import org.apache.spark.sql.SparkSession
import nl.basjes.parse.useragent.UserAgentAnalyzer
import org.apache.spark.sql._
import org.apache.spark.sql.functions.desc


object OSAnalysis {

  def getDeviceInfo(row: Row): (String, String, String, String) = {
    val uaa = UserAgentAnalyzer
      .newBuilder()
      .hideMatcherLoadStats()
      .withCache(10000)
      .build()
    val result = uaa.parse(row.getAs[String]("user_agent"))
    (row.getAs[String]("user_id"),
      result.getValue("DeviceName"),
      result.getValue("AgentName"),
      result.getValue("OperatingSystemNameVersionMajor"))
  }


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
    websiteLogs.show()
    val total = websiteLogs.count()
    val withDevices = websiteLogs
      .map(row => getDeviceInfo(row)).toDF("user_id", "device", "browser", "os")
      .cache()
    withDevices.show()

    val stats = withDevices.groupBy("os").count()
      .map(row => (row.getAs[String]("os"),
        row.getAs[Long]("count"),
        (row.getAs[Long]("count") / total.toDouble) * 100))
      .toDF("Operating System", "Users", "% Users")
      .orderBy(desc("Users"))

    stats.show()
    spark.stop()


  }

}

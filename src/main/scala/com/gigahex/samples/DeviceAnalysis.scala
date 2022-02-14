package com.gigahex.samples


import nl.basjes.parse.useragent.UserAgentAnalyzer
import org.apache.spark.sql._
import org.apache.spark.sql.functions.desc


object DeviceAnalysis {

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

    val websiteLogs = spark.read.json("/Users/gigahex/logs_devices.json")
    val total = websiteLogs.count()
    val withDevices = websiteLogs
      .map(row => getDeviceInfo(row)).toDF("user_id", "device", "browser", "os")
    withDevices.show()

    val stats = withDevices.groupBy("browser").count()
      .map(row => (row.getAs[String]("browser"),
        row.getAs[Long]("count"),
        (row.getAs[Long]("count") / total.toDouble) * 100))
      .toDF("Browser", "Users", "% Users")
      .orderBy(desc("Users"))

    stats.show()


    spark.stop()


  }

}

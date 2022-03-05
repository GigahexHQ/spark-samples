package com.gigahex.samples.analytics

import java.io.{BufferedReader, InputStreamReader}
import java.net.{HttpURLConnection, URL}

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.SparkSession


object LogsAnalysis {

  val logger = LogFactory.getLog(getClass)

  def getCountry(ip: String): String = {

    val url = s"https://tools.keycdn.com/geo.json?host=${ip}"
    val USER_AGENT = "keycdn-tools:https://www.example.com"

    val obj = new URL(url)
    val con = obj.openConnection.asInstanceOf[HttpURLConnection]
    con.setConnectTimeout(1000)
    con.setRequestProperty("User-Agent", USER_AGENT)

    val in = new BufferedReader(new InputStreamReader(con.getInputStream))
    var inputLine: String = null
    val response = new StringBuffer
    inputLine = in.readLine()
    while(inputLine != null){
      response.append(inputLine)
      inputLine = in.readLine()
    }

    in.close()
    val mapper = new ObjectMapper()
    mapper.readTree(response.toString).path("data").path("geo").path("country_name").toString

  }

  def main(args: Array[String]): Unit = {
    /**
     * Setup the spark session
     */
    val spark = SparkSession.builder()
      .appName("count-words-scala")
      .getOrCreate()
    import spark.implicits._

    val websiteLogs = spark.read.json("/Users/gigahex/logs.json")
    val withCountry = websiteLogs.map(row => (
      row.getAs[String]("user_id"),
      row.getAs[String]("ip_addr"),
      getCountry(row.getAs[String]("ip_addr")))).toDF("user_id","ip", "country")
    val total = withCountry.count()
    val stats = withCountry.groupBy("country")
      .count()
      .map(row => (row.getAs[String]("country"),
        row.getAs[Long]("count"),
        (row.getAs[Long]("count")/total.toDouble)*100))
      .toDF("Country","Users", "% Users")

    stats.show()

    spark.stop()
  }

}

package com.gigahex.samples.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.listeners.SparkMetricsListener

object SparkApp {

  def main(args: Array[String]): Unit = {
    /**
     * Setup the spark session
     */
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("hello-spark")
      .getOrCreate()

    spark.sparkContext.addSparkListener(new SparkMetricsListener)

    val rdd = spark.sparkContext.textFile(getClass.getResource("/data.in").getPath)
    println(rdd.count())

    spark.stop()
  }

}

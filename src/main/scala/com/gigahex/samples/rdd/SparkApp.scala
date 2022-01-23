package com.gigahex.samples.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.listeners.SparkMetricsListener

object SparkApp {

  def main(args: Array[String]): Unit = {
    /**
     * Setup the spark session
     */
    val spark = SparkSession.builder()
      .master("local")
      .config("spark.cores.max", "2")
      .config("spark.executor.memory", "512m")
      .appName("spark-listener-example")
      .getOrCreate()

    spark.sparkContext.addSparkListener(new SparkMetricsListener)

    val rdd = spark.sparkContext.textFile(args(0))
    println(rdd.count())

    spark.stop()
  }

}

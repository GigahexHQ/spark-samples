package com.gigahex.samples.s3

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object S3IO {

  def main(args: Array[String]): Unit = {

    if(args.length < 2){
      System.err.println("Usage: -f <file-path> -o <output-path>")
      sys.exit(1)
    }

    val fileArg = args.indexOf("-f") + 1
    val outArg = args.indexOf("-o") + 1

    /**
     * Setup the spark session
     */
    val spark = SparkSession.builder()
      .master("local")
      .config("spark.hadoop.fs.s3a.access.key", System.getProperty("aws.key"))
      .config("spark.hadoop.fs.s3a.secret.key", System.getProperty("aws.secret"))
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .appName("spark-s3-read-write")
      .getOrCreate()



    spark.read.json(args(fileArg))
      .groupBy("source")
      .agg(count("id") as "flights_count")
      .select("source","flights_count")
      .withColumnRenamed("source", "city")
      .write.csv(args(outArg))

    spark.stop()
  }

}

package com.gigahex.samples.frameless

import java.sql.Timestamp

import org.apache.spark.listeners.SparkMetricsListener
import org.apache.spark.sql.{Row, SparkSession}
import frameless.functions.aggregate._
import frameless.{Injection, TypedDataset}

object FramelessDemo {

  def main(args: Array[String]): Unit = {
    /**
     * Setup the spark session
     */
    implicit val spark = SparkSession.builder()
      .master("local[*]")
      .appName("hello-frameless")
      .getOrCreate()

    implicit val RowToFlight = new Injection[Row, Flight] {
      override def apply(a: Row): Flight = ???

      override def invert(b: Flight): Row = ???
    }

    import spark.implicits._

    val flights = spark.read.json(FramelessDemo.getClass.getResource("/frameless/flights.in").getPath).as[Flight]


    spark.stop()
  }

}

//Find out the number of guests traveling from New York To London whose age is > 40
case class Flight(id: Long, source: String, destination: String, departureTime: Timestamp, arrivalTime: Timestamp)
case class TravelGuest(name: String, age: Int)
case class Trip(pnr: String, flightId: Long, guests: Seq[TravelGuest])

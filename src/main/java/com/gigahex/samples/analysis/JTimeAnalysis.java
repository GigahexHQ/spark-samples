package com.gigahex.samples.analysis;


import nl.basjes.parse.useragent.UserAgent;
import nl.basjes.parse.useragent.UserAgentAnalyzer;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;
import scala.Tuple3;
import scala.Tuple4;

import java.io.IOException;

import static org.apache.spark.sql.functions.desc;

public class JTimeAnalysis {

    public static void main(String[] args) throws Exception {

        //Initialize the spark session
        SparkSession spark = SparkSession.builder()
                .appName("usage-by-time")
                .master("local")
                .getOrCreate();

        StructType structDevice = new StructType();
        structDevice = structDevice.add("user_id", DataTypes.StringType, false);
        structDevice = structDevice.add("device", DataTypes.StringType, false);
        structDevice = structDevice.add("browser", DataTypes.StringType, false);
        structDevice = structDevice.add("os", DataTypes.StringType, false);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(structDevice);

        StructType statsStruct = new StructType();
        statsStruct = statsStruct.add("Time Range", DataTypes.StringType, false);
        statsStruct = statsStruct.add("Users", DataTypes.LongType, false);
        statsStruct = statsStruct.add("% Users", DataTypes.DoubleType, false);
        ExpressionEncoder<Row> encoderStats = RowEncoder.apply(statsStruct);


        //Create a dataset by reading the input file

        Dataset<Row> websiteLogs = spark.read().json("hdfs://0.0.0.0:9075/user/gigahex/logs_devices.json");
        Long total = websiteLogs.count();

        Dataset<Row> withHours = websiteLogs.withColumn("hour", hour(from_unixtime(col("timestamp")))).cache();

        Dataset<Row> stats =   withHours.withColumn("range",
                concat(col("hour"),
                        lit(" to "),
                        col("hour").plus(1)
                ))
                .groupBy("range")
                .count()
                .map((MapFunction<Row, Row>) row -> Row.fromTuple(new Tuple3<String, Long, Double>(
                        row.getAs("range"),
                        row.getAs("count"),
                        (row.<Long>getAs("count").doubleValue() / total) * 100)), encoderStats)
                .orderBy(desc("users"));

        stats.show();

        spark.stop();
    }
}

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
import scala.Tuple3;
import scala.Tuple4;

import java.io.IOException;

import static org.apache.spark.sql.functions.desc;

public class JOperatingSystemAnalysis {

    static Row getDeviceInfo(Row row) throws IOException {
        UserAgentAnalyzer uaa = UserAgentAnalyzer
                .newBuilder()
                .hideMatcherLoadStats()
                .withCache(10000)
                .build();
        UserAgent.ImmutableUserAgent result = uaa.parse(row.<String>getAs("user_agent"));


        return Row.fromTuple(
                new Tuple4<String, String, String, String>(
                        row.getAs("user_id"),
                        result.getValue("DeviceName"),
                        result.getValue("AgentName"),
                        result.getValue("OperatingSystemNameVersionMajor")));
    }

    public static void main(String[] args) throws Exception {

        //Initialize the spark session
        SparkSession spark = SparkSession.builder()
                .appName("count-device-usage")
                .master("local")
                .getOrCreate();

        StructType structDevice = new StructType();
        structDevice = structDevice.add("user_id", DataTypes.StringType, false);
        structDevice = structDevice.add("device", DataTypes.StringType, false);
        structDevice = structDevice.add("browser", DataTypes.StringType, false);
        structDevice = structDevice.add("os", DataTypes.StringType, false);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(structDevice);

        StructType statsStruct = new StructType();
        statsStruct = statsStruct.add("Operating System", DataTypes.StringType, false);
        statsStruct = statsStruct.add("users", DataTypes.LongType, false);
        statsStruct = statsStruct.add("% Users", DataTypes.DoubleType, false);
        ExpressionEncoder<Row> encoderStats = RowEncoder.apply(statsStruct);


        //Create a dataset by reading the input file
        Dataset<Row> websiteLogs = spark.read().json("hdfs://0.0.0.0:9075/user/gigahex/logs_devices.json");
        Dataset<Row> withDevices = websiteLogs.map((MapFunction<Row, Row>) row -> getDeviceInfo(row), encoder);

        Long total = websiteLogs.count();

        Dataset<Row> stats = withDevices.groupBy("os")
                .count()
                .map((MapFunction<Row, Row>) row -> Row.fromTuple(new Tuple3<String, Long, Double>(
                        row.getAs("os"),
                        row.getAs("count"),
                        (row.<Long>getAs("count").doubleValue() / total) * 100)), encoderStats)
                .orderBy(desc("users"));

        stats.show();

        spark.stop();
    }
}

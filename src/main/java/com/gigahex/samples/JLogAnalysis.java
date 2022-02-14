package com.gigahex.samples;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;

import java.net.URL;


public class JLogAnalysis {

    static String getCountry(String ip) throws IOException {

        String url = "https://tools.keycdn.com/geo.json?host=" + ip;
        String USER_AGENT = "keycdn-tools:https://www.example.com";

        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setConnectTimeout(1000);
        con.setRequestProperty("User-Agent", USER_AGENT);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(response.toString()).path("data").path("geo").path("country_name").toString();

    }

    public static void main(String[] args) throws Exception {

        //Initialize the spark session
        SparkSession spark = SparkSession.builder()
                .appName("count-words-java")
                .master("local")
                .getOrCreate();

        StructType structCountry = new StructType();
        structCountry = structCountry.add("ip_addr", DataTypes.StringType, false);
        structCountry = structCountry.add("user_id", DataTypes.StringType, false);
        structCountry = structCountry.add("country", DataTypes.StringType, false);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(structCountry);

        StructType statsStruct = new StructType();
        statsStruct = statsStruct.add("country", DataTypes.StringType, false);
        statsStruct = statsStruct.add("users", DataTypes.LongType, false);
        statsStruct = statsStruct.add("Percentage Users", DataTypes.DoubleType, false);
        ExpressionEncoder<Row> encoderStats = RowEncoder.apply(statsStruct);

        //Create a dataset by reading the input file
        Dataset<Row> websiteLogs = spark.read().json("/Users/shad/logs.json");
        Dataset<Row> withCountry = websiteLogs.map((MapFunction<Row, Row>) row -> Row.fromTuple(
                new Tuple3<String, String, String>(
                        row.getAs("ip_addr"),
                        row.getAs("user_id"),
                        getCountry(row.getAs("ip_addr")))), encoder);

        Long total = withCountry.count();
        Dataset<Row> stats = withCountry.groupBy("country")
                .count()
                .map((MapFunction<Row, Row>) row -> Row.fromTuple(new Tuple3<String, Long, Double>(
                        row.getAs("country"),
                        row.getAs("count"),
                        (row.<Long>getAs("count").doubleValue() / total) * 100)), encoderStats);

        stats.show();

        spark.stop();
    }
}

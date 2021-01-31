package com.zhiwei.flink.practice.tablesql.example;

import com.zhiwei.flink.practice.tablesql.utils.GeoUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class PopularPlacesTableApi {
    public static void main(String[] args) throws Exception {

        // read parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.getRequired("input");

        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create a TableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // register TaxiRideTableSource as table "TaxiRides"
//        tEnv.registerTableSource(
//                "TaxiRides",
//                new TaxiRideTableSource(
//                        input,
//                        maxEventDelay,
//                        servingSpeedFactor));

        // register user-defined functions
        tEnv.createFunction("isInNYC", GeoUtils.IsInNYC.class);
        tEnv.createFunction("toCellId", GeoUtils.ToCellId.class);
        tEnv.createFunction("toCoords", GeoUtils.ToCoords.class);


        Table popPlaces = tEnv
                .from("TaxiRides")
                // scan TaxiRides table
                // filter for valid rides
                .filter($("isInNYC(startLon, startLat) && isInNYC(endLon, endLat)"))
                // select fields and compute grid cell of departure or arrival coordinates
                .select($("eventTime, " +
                        "isStart, " +
                        "(isStart = true).?(toCellId(startLon, startLat), toCellId(endLon, endLat)) AS cell"))
                // specify sliding window over 15 minutes with slide of 5 minutes
                .window(Slide.over($("15.minutes")).every($("5.minutes"))
                        .on($("eventTime")).as("w"))
                // group by cell, isStart, and window
                .groupBy($("cell, isStart, w"))
                // count departures and arrivals per cell (location) and window (time)
                .select($("cell, isStart, w.start AS start, w.end AS end, count(isStart) AS popCnt"))
                // filter for popular places
                .filter($("popCnt > 20"))
                // convert cell back to coordinates
                .select($("toCoords(cell) AS location, start, end, isStart, popCnt"));

        // convert Table into an append stream and print it
        tEnv.toAppendStream(popPlaces, Row.class).print();

        // execute query
        env.execute();
    }

}

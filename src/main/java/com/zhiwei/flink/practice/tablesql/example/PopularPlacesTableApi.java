package com.zhiwei.flink.practice.tablesql.example;

import com.zhiwei.flink.practice.tablesql.utils.GeoUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class PopularPlacesTableApi {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create a TableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // register user-defined functions
        tEnv.createFunction("isInNYC", GeoUtils.IsInNYC.class);
        tEnv.createFunction("toCellId", GeoUtils.ToCellId.class);
        tEnv.createFunction("toCoords", GeoUtils.ToCoords.class);


        Table popPlaces = tEnv
                .from("TaxiRides")
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

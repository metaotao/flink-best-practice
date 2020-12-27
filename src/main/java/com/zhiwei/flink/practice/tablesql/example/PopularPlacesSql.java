package com.zhiwei.flink.practice.tablesql.example;

import com.zhiwei.flink.practice.tablesql.source.TaxiRideTableSource;
import com.zhiwei.flink.practice.tablesql.utils.GeoUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class PopularPlacesSql {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.getRequired("input");

        final int maxEventDelay = 60;       	// events are out of order by max 60 seconds
        final int servingSpeedFactor = 1800; 	// events of 30 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // create a TableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // TODO register TaxiRideTableSource as table "TaxiRides"
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

        Table results = tEnv.sqlQuery(
                //"SELECT TUMBLE_START(eventTime, INTERVAL '1' HOUR), isStart, count(isStart) FROM TaxiRides GROUP BY isStart, TUMBLE(eventTime, INTERVAL '1' HOUR)"
                //"SELECT avg(endTime - startTime), passengerCnt FROM TaxiRides GROUP BY passengerCnt"
                "SELECT CAST (toCellId(endLon, endLat) AS VARCHAR), eventTime," +
                        "COUNT(*) OVER (" +
                        "PARTITION BY toCellId(endLon, endLat) ORDER BY eventTime RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW" +
                        ") " +
                        "FROM( SELECT * FROM TaxiRides WHERE not isStart AND toCellId(endLon, endLat) = 50801 )"
        );

        // convert Table into an append stream and print it
        // (if instead we needed a retraction stream we would use tEnv.toRetractStream)
        tEnv.toRetractStream(results, Row.class).print();

        // execute query
        env.execute();
    }

}

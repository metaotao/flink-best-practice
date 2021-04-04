package com.zhiwei.flink.practice.tablesql.example;

import com.zhiwei.flink.practice.tablesql.utils.GeoUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class PopularPlacesSql {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

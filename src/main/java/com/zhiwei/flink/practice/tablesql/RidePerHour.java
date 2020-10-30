package com.zhiwei.flink.practice.tablesql;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RidePerHour {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String input = parameterTool.getRequired("input");
        final int maxEventDelay = 60;       	// events are out of order by max 60 seconds
        final int servingSpeedFactor = 1800; 	// events of 30 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create a TableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    }
}

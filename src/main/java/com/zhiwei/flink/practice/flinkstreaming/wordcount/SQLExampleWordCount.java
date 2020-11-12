package com.zhiwei.flink.practice.flinkstreaming.wordcount;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SQLExampleWordCount {

    public static void main(String[] args) {

        StreamExecutionEnvironment blinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkEnv.setParallelism(1);
        EnvironmentSettings blinkSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(blinkEnv, blinkSettings);

    }
}

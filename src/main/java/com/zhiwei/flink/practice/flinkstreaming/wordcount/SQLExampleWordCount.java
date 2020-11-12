package com.zhiwei.flink.practice.flinkstreaming.wordcount;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

public class SQLExampleWordCount {

    public static void main(String[] args) {

        StreamExecutionEnvironment blinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkEnv.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();

    }
}

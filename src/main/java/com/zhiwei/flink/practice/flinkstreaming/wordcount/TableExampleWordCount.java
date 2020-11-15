package com.zhiwei.flink.practice.flinkstreaming.wordcount;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TableExampleWordCount {
    public static void main(String[] args) {
        StreamExecutionEnvironment blinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkEnv.setParallelism(1);
        EnvironmentSettings blinkSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(blinkEnv, blinkSettings);
        DataStream<String> dataStream= blinkEnv.readTextFile(TableExampleWordCount.class.getClassLoader().getResource("words.txt").getPath());
        tableEnv.createTemporaryView("outputTable", dataStream);

        Table wordCountTable = tableEnv.from("outputTable")
                .groupBy($("word"))
                .select($("word").count().as("word", "_count"));

        tableEnv.toRetractStream(wordCountTable, Row.class).print();
        tableEnv.executeSql("execute table");

    }
}

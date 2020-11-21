package com.zhiwei.flink.practice.flinkstreaming.wordcount;

import com.zhiwei.flink.practice.flinkstreaming.wordcount.bean.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TableExampleWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkEnv.setParallelism(1);
        EnvironmentSettings blinkSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(blinkEnv, blinkSettings);

        DataStream<String> dataStream= blinkEnv.readTextFile(TableExampleWordCount.class.getClassLoader().getResource("words.txt").getPath());
        DataStream<Data> dataDataStream = dataStream.map((MapFunction<String, Data>) Data::new);
        tableEnv.createTemporaryView("outputTable", dataDataStream, $("word"));

        Table wordCountTable = tableEnv.from("outputTable")
                .groupBy($("word"))
                .select($("word").count().as("word", "_count"));

        tableEnv.toRetractStream(wordCountTable, Row.class).print();
        blinkEnv.execute("execute");

    }
}

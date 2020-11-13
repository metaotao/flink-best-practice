package com.zhiwei.flink.practice.flinkstreaming.wordcount;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class SQLExampleWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment blinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkEnv.setParallelism(1);
        EnvironmentSettings blinkSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(blinkEnv, blinkSettings);
        String path = SQLExampleWordCount.class.getClassLoader().getResource("words.txt").getPath();

        CsvTableSource csvTableSource = CsvTableSource.builder()
                .field("word", Types.STRING)
                .path(path)
                .build();
        blinkTableEnv.registerTableSource("zhiwei", csvTableSource);
        Table wordWithCount = blinkTableEnv.sqlQuery("SELECT count(word), word FROM zhisheng GROUP BY word");
        blinkTableEnv.toRetractStream(wordWithCount, Row.class).print();

        blinkTableEnv.execute("Blink Stream SQL Job");
    }
}

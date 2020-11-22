package com.zhiwei.flink.practice.flinkstreaming.wordcount;


import com.zhiwei.flink.practice.flinkstreaming.wordcount.bean.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;
import org.apache.flink.types.Row;

import java.time.Duration;

public class SQLExampleWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment blinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkEnv.setParallelism(1);
        EnvironmentSettings blinkSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(blinkEnv, blinkSettings);

        blinkTableEnv.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        blinkTableEnv.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));

        String path = "/Users/taojiayun/zhiwei/project/flink-best-practice/src/main/resources/words.txt";
        DataStream<String> inputStream= blinkEnv.readTextFile(path);

        DataStream<Data> dataDataStream = inputStream.map((MapFunction<String, Data>) Data::new);

        blinkTableEnv.createTemporaryView("zhiwei", dataDataStream,$("word"));
        Table wordWithCount = blinkTableEnv.sqlQuery("SELECT count(word), word FROM zhiwei GROUP BY word");
//        blinkTableEnv.sqlQuery("SELECT word FROM zhiwei").execute().print();


        blinkTableEnv.toRetractStream(wordWithCount, Row.class).print("result");
        blinkEnv.execute("sql execute");
    }
}

package com.zhiwei.flink.practice.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaDataToEs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkStreamEnv.setParallelism(1);
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv, blinkStreamSettings);

        String ddlSource = "CREATE TABLE user_behavior (\n" +
                "    userDetail  Row<userId BIGINT, name STRING, age BIGINT>,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = '0.11',\n" +
                "    'connector.topic' = 'user_behavior',\n" +
                "    'connector.startup-mode' = 'latest-offset',\n" +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'format.type' = 'json'\n" +
                ")";

        String ddlSink = "CREATE TABLE user_behavior_es (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT\n" +
                ") WITH (\n" +
                "    'connector.type' = 'elasticsearch',\n" +
                "    'connector.version' = '6',\n" +
                "    'connector.hosts' = 'http://localhost:9200',\n" +
                "    'connector.index' = 'user_behavior_es',\n" +
                "    'connector.document-type' = 'user_behavior_es',\n" +
                "    'format.type' = 'json',\n" +
                "    'update-mode' = 'append',\n" +
                "    'connector.bulk-flush.max-actions' = '10'\n" +
                ")";

        //提取读取到的数据，然后只要两个字段，写入到 ES
        String sql = "insert into user_behavior_es select userDetail.userId, item_id from user_behavior";
        System.out.println(ddlSource);
        System.out.println(ddlSink);
        blinkStreamTableEnv.executeSql(ddlSource);
        blinkStreamTableEnv.executeSql(ddlSink);
        blinkStreamTableEnv.executeSql(sql);

        blinkStreamTableEnv.execute("Blink Stream SQL Job2 —— read data from kafka，sink to es");
    }
}

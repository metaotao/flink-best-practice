package com.zhiwei.flink.practice.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaDataToHbase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkStreamEnv.setParallelism(1);
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv, blinkStreamSettings);

        String ddlSource = "CREATE TABLE user_behavior (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = '0.11',\n" +
                "    'connector.topic' = 'user_behavior',\n" +
                "    'connector.startup-mode' = 'latest-offset',\n" +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'format.type' = 'json'\n" +
                ")";

        String ddlSink = "CREATE TABLE user_behavior_hbase (\n" +
                "  rowkey BIGINT,\n" +
                "  cf ROW<item_id BIGINT, category_id BIGINT>\n" +
                ") WITH (\n" +
                "  'connector.type' = 'hbase',\n" +
                "  'connector.version' = '1.4.3',\n" +
                "  'connector.table-name' = 'zhiwei',\n" +
                "  'connector.zookeeper.quorum' = 'localhost:2181',\n" +
                "  'connector.zookeeper.znode.parent' = '/hbase',\n" +
                "  'connector.write.buffer-flush.max-size' = '2mb',\n" +
                "  'connector.write.buffer-flush.max-rows' = '1000',\n" +
                "  'connector.write.buffer-flush.interval' = '2s'\n" +
                ")";

        //提取读取到的数据，然后只要两个字段，写入到 HBase
        String sql = "insert into user_behavior_hbase select user_id, ROW(item_id, category_id) from user_behavior";

        System.out.println(ddlSource);
        System.out.println(ddlSink);
        blinkStreamTableEnv.executeSql(ddlSource);
        blinkStreamTableEnv.executeSql(ddlSink);
        blinkStreamTableEnv.executeSql(sql);

        blinkStreamTableEnv.execute("Blink Stream SQL Job5 —— read data from kafka，sink to HBase");
    }
}

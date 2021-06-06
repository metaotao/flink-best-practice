package com.zhiwei.flink.practice.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ExecutionPlanTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // register a table named "Orders"
        tEnv.executeSql("CREATE TABLE MyTable1 (`count` bigint, word VARCHAR(256) WITH (...)");
        tEnv.executeSql("CREATE TABLE MyTable2 (`count` bigint, word VARCHAR(256) WITH (...)");

        // explain SELECT statement through TableEnvironment.explainSql()
        String explanation = tEnv.explainSql(
                "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' " +
                        "UNION ALL " +
                        "SELECT `count`, word FROM MyTable2");
        System.out.println(explanation);

        // explain SELECT statement through TableEnvironment.executeSql()
        TableResult tableResult = tEnv.executeSql(
                "EXPLAIN PLAN FOR " +
                        "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' " +
                        "UNION ALL " +
                        "SELECT `count`, word FROM MyTable2");
        tableResult.print();
    }
}

package com.zhiwei.flink.practice.connector.datastream.jdbc;

import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Types;

public class JdbcConnector {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JdbcBatchingOutputFormat jdbcOutputFormat =
                JdbcBatchingOutputFormat.builder()
                        .setOptions(
                                JdbcOptions.builder()
                                        .setDBUrl("")
                                        .setTableName("")
                                        .build())
                        .setFieldNames(new String[] {"id", "title", "author", "price", "qty"})
                        .setFieldTypes(
                                new int[] {
                                        Types.INTEGER,
                                        Types.VARCHAR,
                                        Types.VARCHAR,
                                        Types.DOUBLE,
                                        Types.INTEGER
                                })
                        .setKeyFields(null)
                        .build();

        env.execute();

    }
}

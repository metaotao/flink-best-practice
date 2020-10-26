package com.zhiwei.flink.practice.tablesql;

import org.apache.flink.api.java.utils.ParameterTool;

public class RidePerHour {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String input = parameterTool.getRequired("input");

    }
}

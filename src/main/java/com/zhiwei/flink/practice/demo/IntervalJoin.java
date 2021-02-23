package com.zhiwei.flink.practice.demo;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class IntervalJoin {

    public static void main(String[] args) throws IOException {
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(fromArgs.getRequired("application"));

        String kafkaBootstrapServers = parameterTool.getRequired("kafkaBootstrapServers");

        String browseTopic = parameterTool.getRequired("browseTopic");
        String browseTopicGroupID = parameterTool.getRequired("browseTopicGroupID");

        String clickTopic = parameterTool.getRequired("clickTopic");
        String clickTopicGroupID = parameterTool.getRequired("clickTopicGroupID");

        //2、设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    }


    private class ClickLog {

    }
}



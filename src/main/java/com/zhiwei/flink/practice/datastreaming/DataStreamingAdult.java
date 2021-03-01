package com.zhiwei.flink.practice.datastreaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamingAdult {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> personDataStream = env.fromElements(new Person("Bob", 28));

        DataStream<Person> filterPerson = personDataStream.filter(
                (FilterFunction<Person>) person -> person.age > 18);

        filterPerson.print();
        env.execute();
    }

    public static class Person {
        public String name;
        public Integer age;

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        };

    }
}

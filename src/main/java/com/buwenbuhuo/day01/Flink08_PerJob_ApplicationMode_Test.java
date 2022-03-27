package com.buwenbuhuo.day01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-25 15:38
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:
 */
public class Flink08_PerJob_ApplicationMode_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test1(env);
        test2(env);
        test3(env);
    }

    public static void test1(StreamExecutionEnvironment env) throws Exception {

        DataStreamSource<String> stringDataStreamSource = env.fromElements("22222");
        stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
        env.execute();
    }

    public static void test2(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> stringDataStreamSource = env.fromElements("22222");
        stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
        env.execute();
    }

    public static void test3(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("hadoop01", 9999);
        stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
        env.execute();
    }

}


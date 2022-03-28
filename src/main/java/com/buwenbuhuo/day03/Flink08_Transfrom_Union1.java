package com.buwenbuhuo.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-28 14:17
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:Union的匿名内部类写法
 */
public class Flink08_Transfrom_Union1 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);

        // TODO 2.核心代码
        /**
         * 1.从元素种获取数据，并创建两条流
         *    intDStream:整数型
         *    strDStream:字符串类型
         */
        DataStreamSource<String> intDStream = env.fromElements("1", "2", "3", "4", "5", "6");
        DataStreamSource<String> strDStream = env.fromElements("a", "b", "c", "d", "e");

        // 2.使用unoin连接两条流
        DataStream<String> unionDStream = intDStream.union(strDStream);

        SingleOutputStreamOperator<String> result = unionDStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value + " unoin";
            }
        });

        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}

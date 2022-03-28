package com.buwenbuhuo.day03;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * Author 不温卜火
 * Create 2022-03-28 14:08
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:connect的匿名内部类写法
 */
public class Flink07_Transfrom_Connect {
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
        DataStreamSource<Integer> intDStream = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<String> strDStream = env.fromElements("a", "b", "c", "d", "e");

        // 2.使用connect连接两条流
        ConnectedStreams<Integer, String> connectDSream = intDStream.connect(strDStream);

        SingleOutputStreamOperator<String> result = connectDSream.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value * value + " first";
            }
            @Override
            public String map2(String value) throws Exception {
                return value + " second";
            }
        });

        // 输出
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}

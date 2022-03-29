package com.buwenbuhuo.day04;

import com.buwenbuhuo.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-29 16:14
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:PV的WordCount实现
 */
public class Flink07_Project_PV1 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.并行度设置为1
        env.setParallelism(1);
        // 3.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        // TODO 2.核心代码
        // 1.将数据转为JavaBean
        SingleOutputStreamOperator<UserBehavior> userBehaviorDStream = streamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );
            }
        });

        // 2.过滤出PV数据
        SingleOutputStreamOperator<UserBehavior> pvDStream = userBehaviorDStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        // 3.将过滤出的数组组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> pvToOneDStream = pvDStream.
                map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return Tuple2.of("pv", 1);
            }
        });

        // 4.将相同的key聚合到一起
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = pvToOneDStream.keyBy(0);

        // 5.累加计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        // 6.打印输出
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}

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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * Author 不温卜火
 * Create 2022-03-29 16:14
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:UV的WordCount实现
 */
public class Flink08_Project_UV1 {
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

        // 3.将数据组成Tuple2元组key："UV",value:userId
        SingleOutputStreamOperator<Tuple2<String, Long>> uvToUserIdDStream = pvDStream.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of("uv", value.getUserId());
            }
        });


        // 4.获取UV个数(不聚合数据可能会出错)
        /*SingleOutputStreamOperator<Tuple2<String, Long>> result = uvToUserIdDStream.process(new ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            HashSet<Long> set = new HashSet<>();

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out)
                    throws Exception {
                // 将数据userid存入set集合
                set.add(value.f1);
                // 获取集合的元素个数
                int size = set.size();
                // 强转类型
                out.collect(Tuple2.of("uv", Long.parseLong(size + "")));
            }
        });*/

        // TODO 对相同的key的数据聚合到一块，这样做可以设置多并行度
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = uvToUserIdDStream.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String, Integer>>() {
            HashSet<Long> set = new HashSet<>();

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 将数据userid存入set集合
                set.add(value.f1);
                // 获取集合的元素个数
                int size = set.size();
                // 强转类型
                out.collect(Tuple2.of("uv", size));
            }
        });

        // 5.打印输出
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}

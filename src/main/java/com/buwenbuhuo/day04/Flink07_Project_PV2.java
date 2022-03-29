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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Author 不温卜火
 * Create 2022-03-29 16:14
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:PV的process实现
 */
public class Flink07_Project_PV2 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.并行度设置为1
        env.setParallelism(1);
        // 3.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        // TODO 2.核心代码
        // 1.使用process求出pv
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = streamSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            // 声明一个累加器用来保存个数
            private Integer count = 0;

            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                UserBehavior userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );
                if ("pv".equals(userBehavior.getBehavior())) {
                    count++;
                    out.collect(Tuple2.of("pv", count));
                }
            }
        });

        // 2.打印输出
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}

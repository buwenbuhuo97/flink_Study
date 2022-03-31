package com.buwenbuhuo.day05;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Author 不温卜火
 * Create 2022-03-30 8:53
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:聚合函数（AggregateFunction）的代码实现
 */
public class Flink08_TimeWindow_Tumbing_AggregateFunction {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1. 将数据组成Tuple,一进多出适合使用flatmap
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDStream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(Tuple2.of(value, 1));
            }
        });

        // 2.将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDStream.keyBy(0);

        // 3.开启一个基于时间的滚动窗口，窗口大小设置为5s
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows
                .of(Time.seconds(5)));

        // 4.利用窗口函数AggregateFunction实现Sum操作
        SingleOutputStreamOperator<Integer> result = window.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
            /**
             * 初始化累加器
             * @return
             */
            @Override
            public Integer createAccumulator() {
                System.out.println("初始化累加器");
                return 0;
            }

            /**
             * 累加操作，相当于给累加器赋值
             * @param value
             * @param accumulator
             * @return
             */
            @Override
            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                System.out.println("累加操作");
                return accumulator+ value.f1;
            }

            /**
             * 获取最终结果
             * @param accumulator
             * @return
             */
            @Override
            public Integer getResult(Integer accumulator) {
                System.out.println("获取计算结果");
                return accumulator;
            }

            /**
             * 合并累加器，(并无调用，此处没有设计窗口的合并)
             *  只在会话窗口中，因为会话窗口需要对多个窗口进行合并，那么每个窗口中的计算结果也需要合并，
             *  所以在这个方法中合并的累加器，其它窗口不会调用这个方法。
             * @param a
             * @param b
             * @return
             */
            @Override
            public Integer merge(Integer a, Integer b) {
                System.out.println("合并累加器");
                return a + b;
            }
        });

        // 5.打印输出
        result.print();

        // TODO 3.启动执行
        env.execute();

    }
}

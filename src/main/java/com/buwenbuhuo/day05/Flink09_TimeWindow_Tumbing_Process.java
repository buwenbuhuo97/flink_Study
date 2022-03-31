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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Author 不温卜火
 * Create 2022-03-30 8:53
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:处理窗口函数（ProcessWindowFunction）的代码实现
 */
public class Flink09_TimeWindow_Tumbing_Process {
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

        // 4.利用全窗口函数Process实现Sum操作
        SingleOutputStreamOperator<Integer> result = window.process(new ProcessWindowFunction<Tuple2<String, Integer>, Integer, Tuple, TimeWindow>() {

            // 定义一个累加器
            private Integer count = 0;

            /**
             *
             * @param tuple key
             * @param context 上下文对象
             * @param elements 迭代器里面放的是进入窗口的元素
             * @param out 采集器
             * @throws Exception
             */
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Integer> out) throws Exception {
                System.out.println("process......");
                for (Tuple2<String, Integer> element : elements) {
                    count += element.f1;

                }
                out.collect(count);
            }
        });

        // 5.打印输出
        result.print();

        // TODO 3.启动执行
        env.execute();

    }
}

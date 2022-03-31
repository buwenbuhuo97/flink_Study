package com.buwenbuhuo.day05;

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
 * Description:滚动窗口(Tumbling Windows)的代码实现
 */
public class Flink01_TimeWindow_Tumbing {
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
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        // 2.将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDStream.keyBy(0);

        // 3.开启一个基于时间的滚动窗口，窗口大小设置为5s
        /**
         * 浅析 xxx.window 源码：
         *    1.进入window：ctrl + window 找到 WindowAssigner分配器
         *    2.进入WindowAssigner： ctrl + WindowAssigner 进入抽象类(WindowAssigner)
         *    3.查找抽象类的实现类：ctrl + h 找到具体实现类TumblingProcessingTimeWindows
         *    4.找到可以直接new的方法：其构造方法为私有类，所以找到TumblingProcessingTimeWindows方法
         *
         *  具体写法如下：
         *
         *  public static TumblingProcessingTimeWindows of (Time size){
         *      return new TumblingProcessingTimeWindows(size.toMilliseconds(), 0, WindowStagger.ALIGNED);
         *  }
         *
         */
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows
                .of(Time.seconds(5)));

        // 4.确定窗口起止时间，及总条数
        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                String msg =
                        "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                + elements.spliterator().estimateSize() + "条数据 ";
                out.collect(msg);
            }
        });


        // 5.求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = window.sum(1);

        // 6.打印
        process.print();
        result.print();

        // TODO 3.启动执行
        env.execute();

    }
}

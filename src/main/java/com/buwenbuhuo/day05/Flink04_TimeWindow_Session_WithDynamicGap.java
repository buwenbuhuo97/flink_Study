package com.buwenbuhuo.day05;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Author 不温卜火
 * Create 2022-03-30 9:09
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:会话窗口（Session Windows）动态会话间隔的代码实现
 */
public class Flink04_TimeWindow_Session_WithDynamicGap {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1. 将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> wordToOneDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1])*1000, Integer.parseInt(split[2]));
            }
        });

        // 2.将相同的单词聚合到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = wordToOneDStream.keyBy("id");

        // 3.开启一个基于时间的滑动窗口，动态间隔
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(ProcessingTimeSessionWindows
                .withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
                    @Override
                    public long extract(WaterSensor element) {
                        return element.getTs();
                    }
                }));

        // 4.确定窗口起止时间，及总条数
        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<WaterSensor
                , String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg =
                        "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                + elements.spliterator().estimateSize() + "条数据 ";
                out.collect(msg);
            }
        });


        // 5.打印
        process.print();

        // TODO 3.启动执行
        env.execute();
    }
}

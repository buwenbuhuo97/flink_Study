package com.buwenbuhuo.day06;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Author 不温卜火
 * Create 2022-04-01 9:14
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:自定义生成WaterMark:间歇性
 */
public class Flink05_EventTime_WaterWark_Generator2 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 设置WaterMark自动生成周期时间为1s
        env.getConfig().setAutoWatermarkInterval(1000);

        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);
        // DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        // TODO 2.核心代码
        // 1.将数据转为JavaBean，并指定WaterWark
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]) * 1000, Integer.parseInt(split[2]));
            }
        })
                // 指定WaterWark，并设置乱序程度为3秒
                .assignTimestampsAndWatermarks(
                        new WatermarkStrategy<WaterSensor>() {
                            @Override
                            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new MyWaterMarkGenerator(Duration.ofSeconds(3));
                            }
                        }
                                // 分配时间戳
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );

        // 2.对相同Id的数据聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("id");

        // 3.开启一个基于事件时间的滚动窗口
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows
                .of(Time.seconds(5)));

        // 4.确定窗口起止时间，及总条数
        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "当前key：" + tuple.toString() +
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

    // 自定义一个类实现WatermarkGenerator接口
    public static class MyWaterMarkGenerator implements WatermarkGenerator<WaterSensor>{

        private long maxTimestamp;

        private long outOfOrdernessMillis;

        public MyWaterMarkGenerator(Duration maxOutOfOrderness) {

            this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
            // start so that our lowest watermark would be Long.MIN_VALUE.
            this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
        }

        /**
         * 这个方法间歇性调用，如果在这个方法中生成WaterMark那么就是间歇性生成WaterMark
         * @param event
         * @param eventTimestamp
         * @param output
         */
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp,eventTimestamp);
            System.out.println("生成WaterMark："+(maxTimestamp-outOfOrdernessMillis-1));
            output.emitWatermark(new Watermark(maxTimestamp-outOfOrdernessMillis-1));
        }

        /**
         * 这个方法周期性调用，如果在这个方法中生成WaterMark那么就是周期性生成Watermark
         * @param output
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }
}

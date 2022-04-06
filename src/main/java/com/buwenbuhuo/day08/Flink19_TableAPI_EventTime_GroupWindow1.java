package com.buwenbuhuo.day08;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.time.Duration;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Author 不温卜火
 * Create 2022-04-06 11:16
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:Group Windows：滚动窗口（代码实现）
 */
public class Flink19_TableAPI_EventTime_GroupWindow1 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);

        // TODO 2.核心代码
        // 1.获取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        // 2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3.为已有的字段指定事件时间
        Table table = tableEnv.fromDataStream(waterSensorStream
                , $("id"), $("ts").rowtime(), $("vc"));

        // 4.开启一个三秒的滑动窗口
        TableResult result = table
                // 开启一个滚动窗口，滚动窗口为3s
                .window(Tumble.over(lit(3).second()).on($("ts")).as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("vc").sum().as("vcSum"), $("w").start().as("start")
                        , $("w").end().as("end"))
                .execute();

        // 5.打印
        result.print();

    }
}

package com.buwenbuhuo.day07;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-04-02 16:12
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:
 */
public class Flink13_CEP_Condition {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);

        // TODO 2.核心代码
        // 1.从文件读取数据并转为Javabean同时指定waterMark
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env.readTextFile("input/sensor.txt")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]) * 1000, Integer.parseInt(split[2]));
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );

        // 2.定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("start")
                // TODO 迭代条件
                /*.where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> context) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                });*/

                // TODO 简单条件
                /*.where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                });*/

                // TODO 组合条件
                /*.where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> context) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> context) throws Exception {
                        return value.getVc()>30;
                    }
                })
                .or(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> context) throws Exception {
                        return value.getTs()>3000;
                    }
                });*/

                // TODO 停止条件
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> context) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                .timesOrMore(2)
                .until(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> context) throws Exception {
                        return value.getVc() >= 40;
                    }
                });


        // 4.将模式作用于流上
        PatternStream<WaterSensor> patternStream = CEP.pattern(waterSensorDStream, pattern);

        // 5.获取匹配到的数据
        SingleOutputStreamOperator<String> result = patternStream.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                return pattern.toString();
            }
        });

        // 6.输出打印
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}

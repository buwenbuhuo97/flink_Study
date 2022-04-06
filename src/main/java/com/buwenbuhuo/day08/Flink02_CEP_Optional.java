package com.buwenbuhuo.day08;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-04-05 22:05
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:CEP模式知识补充:循环模式的可选性
 */
public class Flink02_CEP_Optional {
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

        /**
         * 未使用可选性结果：
         *      {start=[WaterSensor(id=sensor_1, ts=1000, vc=10), WaterSensor(id=sensor_1, ts=2000, vc=20)], end=[WaterSensor(id=sensor_2, ts=3000, vc=30)]}
         *      {start=[WaterSensor(id=sensor_1, ts=2000, vc=20), WaterSensor(id=sensor_1, ts=4000, vc=40)], end=[WaterSensor(id=sensor_2, ts=5000, vc=50)]}
         *  使用可选性结果：
         *      {start=[WaterSensor(id=sensor_1, ts=1000, vc=10), WaterSensor(id=sensor_1, ts=2000, vc=20)], end=[WaterSensor(id=sensor_2, ts=3000, vc=30)]}
         *      {end=[WaterSensor(id=sensor_2, ts=3000, vc=30)]}
         *      {start=[WaterSensor(id=sensor_1, ts=2000, vc=20), WaterSensor(id=sensor_1, ts=4000, vc=40)], end=[WaterSensor(id=sensor_2, ts=5000, vc=50)]}
         *      {end=[WaterSensor(id=sensor_2, ts=5000, vc=50)]}
         */
        // 2.定义模式  模式知识补充(循环模式的可选性)
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("start")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                .times(2)
                // 模式可选性
                .optional()
                .next("end")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                        return "sensor_2".equals(value.getId());
                    }
                });

        // 3.将模式作用于流上
        PatternStream<WaterSensor> patternStream = CEP.pattern(waterSensorDStream, pattern);

        // 4.获取匹配到的数据
        SingleOutputStreamOperator<String> result = patternStream.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                return pattern.toString();
            }
        });

        // 5.输出打印
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}

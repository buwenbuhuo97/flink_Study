package com.buwenbuhuo.day08;

import com.buwenbuhuo.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-04-05 22:05
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 统计创建订单到下单中间超过15分钟的超时数据以及正常的数据(代码实现)
 */
public class Flink06_CEP_Project_OrderWatch {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);

        // 3.分别获取两个流
        DataStreamSource<String> orderDStream = env.readTextFile("input/OrderLog.csv");

        // TODO 2.核心代码
        // 1.分别将两条流的数据转为JavaBean
        KeyedStream<OrderEvent, Tuple> keyedStream = orderDStream.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3]));
            }
        })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                        return element.getEventTime() * 1000;
                                    }
                                })
                )
                .keyBy("orderId");

        /**
         * 2.定义模式
         *      统计创建订单(匹配数据)
         *      到下单(匹配数据)
         *      中间超过15分钟(within)
         *      的超市数据以及正常的数据(侧输出，分别将两种数据放到不同的流中)
         */
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .next("pay")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.seconds(15));

        // 3.将模式作用域流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, pattern);

        // 4.获取正常的数据以及超时的数据
        SingleOutputStreamOperator<String> result = patternStream.select(new OutputTag<String>("timeout") {},
                new PatternTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                        return pattern.toString();
                    }
                }, new PatternSelectFunction<OrderEvent, String>() {
                    @Override
                    public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                        return pattern.toString();
                    }
                });

        // 5. 打印数据
        result.print("正常数据");
        result.getSideOutput(new OutputTag<String>("timeout"){}).print("超时数据");

        // TODO 3.启动执行
        env.execute();
    }
}

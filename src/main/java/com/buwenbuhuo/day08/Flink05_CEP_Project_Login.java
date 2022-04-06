package com.buwenbuhuo.day08;

import com.buwenbuhuo.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Author 不温卜火
 * Create 2022-04-05 22:05
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 用户2秒内连续两次及以上登录失败则判定为恶意登录(代码实现)
 */
public class Flink05_CEP_Project_Login {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);

        // TODO 2.核心代码
        // 1.从文件读取数据并转为Javabean同时指定waterMark
        KeyedStream<LoginEvent, Tuple> loginDStream = env.readTextFile("input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new LoginEvent(
                                Long.parseLong(split[0]),
                                split[1],
                                split[2],
                                Long.parseLong(split[3]));
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                                    @Override
                                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                        return element.getEventTime() * 1000;
                                    }
                                }))
                .keyBy("userId");

        /**
         * 2. 判断是否恶意登录
         *      用户2秒内（within）
         *      连续（循环模式的连续性，严格连续）
         *      两次及以上（循环两次及两次以上）
         *      登陆失败（通过条件判断事件类型为fail）
         *      则判定为恶意登录
         */
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("fail")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .timesOrMore(2)
                .consecutive()
                .within(Time.seconds(2));

        // 3.检测模式
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginDStream, pattern);

        // 4.获取结果
        SingleOutputStreamOperator<String> result = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> patten) throws Exception {
                return patten.get("fail").toString();
            }
        });

        // 4. 输出打印
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}

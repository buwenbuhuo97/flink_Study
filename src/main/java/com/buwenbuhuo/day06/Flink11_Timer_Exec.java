package com.buwenbuhuo.day06;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Author 不温卜火
 * Create 2022-04-03 18:38
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:连续5秒水位上升案例实现
 */
public class Flink11_Timer_Exec {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.将数据转为JavaBean，并指定WaterWark
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        // 2.对相同Id的数据聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("id");

        // 3.监控水位传感器的水位值，如果水位值在五秒钟之内连续上升则报警，并将报警信息输出到侧输出流
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            // 定义一个变量保存上一次的水位
            private Integer lastVc = Integer.MIN_VALUE;

            // 定义一个变量保存定时器时间
            private Long timer = Long.MIN_VALUE;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // 1.判断水位是否上升
                if (value.getVc() > lastVc) {
                    // 2.水位上升，判断是否已经注册定时器。如果没有注册定时器，则需要注册
                    if (timer == Long.MIN_VALUE) {
                        // 3.没有注册定时器，注册定时器
                        System.out.println("注册定时器：" + ctx.getCurrentKey());
                        timer = ctx.timerService().currentProcessingTime() + 5000;
                        ctx.timerService().registerProcessingTimeTimer(timer);
                    }
                } else {
                    // 4.水位没有上升，删除之前已经注册过的定时器
                    if (timer != Long.MIN_VALUE) {
                        System.out.println("删除定时器：" + ctx.getCurrentKey());
                        ctx.timerService().deleteProcessingTimeTimer(timer);
                        // 5.为了方便下次水位上升时注册，需要把定时时间重置
                        timer = Long.MIN_VALUE;
                    }
                }
                // 6.无论水位是否上升，都要将本次的水位存入lastVc以便下一次水位来做对比
                lastVc = value.getVc();
                out.collect(value.toString());
            }

            /**
             *  写入报警信息
             * @param timestamp
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                ctx.output(new OutputTag<String>("output") {
                }, "警报，水位5秒连续上升");
                // 一旦报警，则重置定时器时间，以便下一个5秒来的数据能够注册定时器
                timer = Long.MIN_VALUE;
                // 重置lastVc
                lastVc = Integer.MIN_VALUE;
            }
        });

        // 4. 打印输出
        result.print("主流");

        // 获取测输出流中的数据并打印
        result.getSideOutput(new OutputTag<String>("output") {
        }).print("侧输出");

        // TODO 3.启动执行
        env.execute();
    }
}

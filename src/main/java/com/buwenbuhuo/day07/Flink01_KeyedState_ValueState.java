package com.buwenbuhuo.day07;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Author 不温卜火
 * Create 2022-04-02 9:16
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 键控状态的使用:ValueState案例
 */
public class Flink01_KeyedState_ValueState {
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

        // 3.案例1：检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            // 定义一个状态用来保存上一次水位值
            private ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // TODO  1.拿着当前的水位和上一次的水位做对比，如果>10则报警
                // 从状态中取出上一次的水位
                Integer lastVc = valueState.value() == null ? value.getVc() : valueState.value();
                if (Math.abs(value.getVc() - lastVc) > 10) {
                    out.collect("水位超过10报警！！！！！！！");
                }
                // TODO 2.将当前水位更新到状态中
                valueState.update(value.getVc());
            }
        });

        // 4.打印输出
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}

package com.buwenbuhuo.day07;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * Description: 键控状态的使用:AggregatingState案例
 */
public class Flink04_KeyedState_AggregatingState {
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

        // 3.案例4：计算每个传感器的平均水位
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            // 创建一个AggregatingState计算平均水位
            private AggregatingState<Integer, Double> aggregatingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer
                        , Tuple2<Integer, Integer>, Double>("agg-State",
                        new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                            @Override
                            public Tuple2<Integer, Integer> createAccumulator() {
                                return Tuple2.of(0, 0);
                            }

                            @Override
                            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                            }

                            @Override
                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                return accumulator.f0 * 1D / accumulator.f1;
                            }

                            @Override
                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                            }
                        }, Types.TUPLE(Types.INT, Types.INT)));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // 1.将数据存入状态中求平均值
                aggregatingState.add(value.getVc());
                // 2.取出状态中计算过后的结果
                Double vcAvg = aggregatingState.get();
                out.collect(value.getId() + "-" + vcAvg);
            }
        });

        // 4.打印输出
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}

package com.buwenbuhuo.day07;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
 * Description: 键控状态的使用:MapState案例
 */
public class Flink05_KeyedState_MapState {
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

        // 3.案例4：去重: 去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            // 创建一个MapState状态
            private MapState<Integer, WaterSensor> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, WaterSensor>
                        ("map-State",Integer.class,WaterSensor.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // 1.判断key是否存在
                if (!mapState.contains(value.getVc())){
                    // 不存在的话将水位存入map状态中
                    mapState.put(value.getVc(), value);
                }
                out.collect(mapState.get(value.getVc()).toString());
            }
        });

        // 4.打印输出
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}

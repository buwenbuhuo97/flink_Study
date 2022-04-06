package com.buwenbuhuo.day03;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * Author 不温卜火
 * Create 2022-03-28 15:01
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:Process匿名实现写法实现累加功能
 */
public class Flink11_Transfrom_Source_Process2 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.使用process将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> WaterSensorDStream = streamSource.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });

        KeyedStream<WaterSensor, Tuple> keyedStream = WaterSensorDStream.keyBy("id");

        // 2.使用process将数据的VC做累加，实现类似Sum的功能
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {

            // 创建一个集合
            private HashMap<String, Integer> map = new HashMap<>();

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                // 判断map集合中是否有对应key的数据
                if (map.containsKey(value.getId())){
                    // TODO 如果key存在
                    // 1.取出上一次累加过后的vc和
                    Integer lastVc = map.get(value.getId());

                    // 2.与当前的vn进行累加
                    int currentVcSum = lastVc + value.getVc();

                    // 3.将当前的vc累加过后的结果重新写入map集合
                    map.put(value.getId(), currentVcSum);

                    // 4.将数据发送到下游
                    out.collect(new WaterSensor(value.getId(), value.getTs(), currentVcSum));
                }else {
                    // TODO key不存在证明这是第一条数据
                    // 将这条数据存入map集合中
                    map.put(value.getId(),value.getVc());
                    out.collect(value);
                }
            }
        });


        // 2.打印
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}

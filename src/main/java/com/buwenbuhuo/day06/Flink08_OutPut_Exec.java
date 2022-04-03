package com.buwenbuhuo.day06;

import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Author 不温卜火
 * Create 2022-04-01 9:14
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 侧输出流把一个流拆成多个流代码实现
 */
public class Flink08_OutPut_Exec {
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
                return new WaterSensor(split[0], Long.parseLong(split[1]) * 1000, Integer.parseInt(split[2]));
            }
        });

        // 2.对相同Id的数据聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("id");

        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("up5"){};

        // 3.采集监控传感器水位值，将水位值高于5cm的值输出到side output
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                // 将所有数据放到主流，另外单独将水位线高于5cm的放到侧输出流中
                if (value.getVc() > 5) {
                    // 获取侧输出
                    ctx.output(outputTag, value);
                    // 将水位小于5cm的放到另一个侧输出流中
                }else if (value.getVc() < 5){
                    ctx.output(new OutputTag<WaterSensor>("low5"){},value);
                }
                out.collect(value);
            }
        });

        // 5.打印主流，实际场景中可以存入Hbase
        result.print("主流");

        // 获取高于5cm侧输出流的数据
        DataStream<WaterSensor> sideOutput = result.getSideOutput(outputTag);
        // 实际场景可以写入es做可视化展示
        sideOutput.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                return JSONObject.toJSONString(value);
            }
        }).print("水位高于5cm");

        // 获取小于5cm的侧输出流数据
        result.getSideOutput(new OutputTag<WaterSensor>("low5"){
        }).print("水位小于5cm");


        // TODO 3.启动执行
        env.execute();


    }
}

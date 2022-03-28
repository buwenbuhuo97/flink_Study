package com.buwenbuhuo.day04;

import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * Author 不温卜火
 * Create 2022-03-28 21:40
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:往kafka写入数据
 */
public class Flink01_Transform_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.map的匿名内部类写法
        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<String,String>() {
            @Override
            public String map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1])
                        , Integer.parseInt(split[2]));
                return JSONObject.toJSONString(waterSensor);
            }
        });

        map.print();

        // 2.将数据写入Kafka
        map.addSink(new FlinkKafkaProducer<String>("hadoop01:9092", "topic_sensor"
                , new SimpleStringSchema()));


        // TODO 3.启动执行
        env.execute();
    }
}

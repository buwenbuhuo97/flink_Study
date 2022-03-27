package com.buwenbuhuo.day02;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-27 18:51
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:从Kafka读取数据(新版写法)
 */
public class Flink04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);

        // 3. 从Kafka读取数据
        /**
         * 各参数：
         *  source： 参数
         *      source为接口类，ctrl+h找到其实现类KafkaSource，ctrl+f12找到KafkaSource，
         *      其作用为获取一个 kafkaSourceBuilder 来构建一个KafkaSource。
         *  WatermarkStrategy:
         *  sourceName: 名称
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop01:9092")
                .setTopics("sensor")
                .setGroupId("220327")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy
                .noWatermarks(), "kafka-source");

        // 4. 打印
        streamSource.print();

        // TODO 2.启动执行
        env.execute();
    }
}

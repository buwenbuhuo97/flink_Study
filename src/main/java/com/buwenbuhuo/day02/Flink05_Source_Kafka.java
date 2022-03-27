package com.buwenbuhuo.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

/**
 * Author 不温卜火
 * Create 2022-03-27 19:23
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:从Kafka读取数据(老版写法)
 */
public class Flink05_Source_Kafka {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);

        // 3. 从Kafka读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.setProperty("group.id", "220327");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema()
                , properties));

        // 4. 打印
        streamSource.print();

        // TODO 2.启动执行
        env.execute();
    }
}

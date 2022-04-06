package com.buwenbuhuo.day08;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import static org.apache.flink.table.api.Expressions.$;


/**
 * Author 不温卜火
 * Create 2022-04-06 11:16
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:通过Connector将数据写入Kafka（代码实现）
 */
public class Flink12_TableAPI_Connect_Kafka_Sink {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);

        // TODO 2.核心代码
        // 1.获取数据
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        // 2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3.将流转换为表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        // 4.对表进行聚合操作
        Table tableResult = table
                .select($("id"), $("ts"), $("vc"));

        // 4.定义表的结构信息
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        // 5.连接外部文件系统，获取数据
        tableEnv.connect(
                new Kafka()
                        // kafka通用版本
                        .version("universal")
                        .topic("sensor")
                        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092"))
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        // 6.把数据写入到输出表中
        tableResult.executeInsert("sensor");

    }
}

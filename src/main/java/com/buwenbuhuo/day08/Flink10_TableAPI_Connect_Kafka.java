package com.buwenbuhuo.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import static org.apache.flink.table.api.Expressions.$;

/**
 * Author 不温卜火
 * Create 2022-04-06 11:16
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:Kafka_source（代码实现）
 */
public class Flink10_TableAPI_Connect_Kafka {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.核心代码
        // 1.定义表的结构信息
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        // 2.连接外部文件系统，获取数据
        tableEnv.connect(new Kafka()
                        // kafka通用版本
                        .version("universal")
                        .topic("sensor")
                        .startFromLatest()
                        .property(ConsumerConfig.GROUP_ID_CONFIG, "bigdata")
                        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092"))
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        // 3. 读取临时表并返回表结果
        Table table = tableEnv.from("sensor");

        // 4.对表进行聚合操作
        Table selectTable = table
                .groupBy($("id"))
                .select($("id"), $("vc").sum());

        // 5.打印
        TableResult execute = selectTable.execute();
        execute.print();

        // TODO 3.启动执行,如果没有调用流中的算子的话可以不用执行此方法
        env.execute();
    }
}

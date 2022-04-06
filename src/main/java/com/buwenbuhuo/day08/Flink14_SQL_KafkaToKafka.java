package com.buwenbuhuo.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * Author 不温卜火
 * Create 2022-04-06 11:16
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: kafka生产消费数据(代码实现)
 */
public class Flink14_SQL_KafkaToKafka {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.核心代码
        // 1.创建一张表消费kafka数据
        tableEnv.executeSql("create table source_sensor (id string, ts bigint, vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_source_sensor',"
                + "'properties.bootstrap.servers' = 'hadoop01:9092',"
                + "'properties.group.id' = 'hadoop',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'csv'"
                + ")");

        // 2.创建一张表将数据写入kafka中
        tableEnv.executeSql("create table sink_sensor(id string, ts bigint, vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_sink_sensor',"
                + "'properties.bootstrap.servers' = 'hadoop01:9092',"
                + "'format' = 'csv'"
                + ")");

        // 3.将查询source_sensor这个表的数据插入到sink_sensor，以此实现将kafka的一个topic的数据写入另一个topic
        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id='sensor_1'");

    }
}

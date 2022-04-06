package com.buwenbuhuo.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author 不温卜火
 * Create 2022-04-06 14:37
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:在创建表的 DDL 中定义处理时间（代码实现）
 */
public class Flink16_SQL_DDL_ProcTime {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.核心代码
        // 1.在建表语句中指定处理时间
        tableEnv.executeSql("create table sensor(id string,ts bigint,vc int,pt_time as PROCTIME()) with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor.txt',"
                + "'format' = 'csv'"
                + ")");

        // 2.输出打印
        TableResult result = tableEnv.executeSql("select * from sensor");
        result.print();
    }
}

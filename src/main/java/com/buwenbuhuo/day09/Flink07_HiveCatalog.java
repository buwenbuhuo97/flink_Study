package com.buwenbuhuo.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * Author 不温卜火
 * Create 2022-04-07 10:08
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: HiveCatalog代码实现
 */
public class Flink07_HiveCatalog {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        //设置用户权限
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.核心代码
        // 1. 创建HiveCatalog
        String name  = "myhive";
        String defaultDatabase = "flink_test";
        String hiveConfDir     = "E:/Bigdata";

        // 2. 创建HiveCatalog
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);

        // 3. 注册HiveCatalog
        tableEnv.registerCatalog(name, hive);

        // 4. 把 HiveCatalog: myhive 作为当前session的catalog
        tableEnv.useCatalog(name);
        tableEnv.useDatabase(defaultDatabase);

        // 5. 查询Hive中表的数据
        tableEnv.sqlQuery("select * from student").execute().print();

    }

}

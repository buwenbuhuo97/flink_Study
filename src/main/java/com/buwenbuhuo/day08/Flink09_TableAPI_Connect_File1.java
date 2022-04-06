package com.buwenbuhuo.day08;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.$;

/**
 * Author 不温卜火
 * Create 2022-04-06 11:16
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:File_source（代码实现）
 */
public class Flink09_TableAPI_Connect_File1 {
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

        /**
         * 2.连接外部文件系统，获取数据
         *      tableEnv.connect(xxx)为老版本的写法，在1.13版本仅保留SQL写法，但是不影响使用
         *      如果想要API式写法，暂时必须要使用老版本的写法
         */
        tableEnv.connect(new FileSystem().path("input/sensor-sql.txt"))
                // 读取文件，并设置行和字段分隔符
                .withFormat(new Csv().lineDelimiter("\n").fieldDelimiter(','))
                // 传入表的结构信息
                .withSchema(schema)
                // 创建临时表并指定表名
                .createTemporaryTable("sensor");

        // 3. 读取临时表并返回表结果
        Table table = tableEnv.from("sensor");

        // 4.对表进行聚合操作
        Table selectTable = table
                .groupBy($("id"))
                .select($("id"), $("vc").sum());

        /**
         * 打印输出（两种方式）：
         *      1.将表转成流打印
         *      2.直接用TotableResult对象打印[推荐此方法]
         */
        // 第一种打印方式：将表转成流打印
        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(selectTable, Row.class);
        dataStream.print();

        // TODO 3.启动执行,如果没有调用流中的算子的话可以不用执行此方法
        env.execute();
    }
}

package com.buwenbuhuo.day08;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.$;

/**
 * Author 不温卜火
 * Create 2022-04-06 10:34
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 聚合操作（代码实现）
 */
public class Flink08_TableAPI_Agg {
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

        // 4.通过连续查询获取数据
        Table resultTable = table
                .groupBy($("id"))
                .select($("id"), $("vc").sum().as("vcSum"));

        /**
         * 5.将表转换为流
         * 聚合时不能使用追加流，使用会报错(toAppendStream)
         *      toAppendStream doesn't support consuming update changes which
         *      is produced by node GroupAggregate(groupBy=[id], select=[id, SUM(vc) AS EXPR$0])
         *  此时需要使用撤回流(toRetractStream)
         */
        DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(resultTable, Row.class);

        // 6.打印
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}

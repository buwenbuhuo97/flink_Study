package com.buwenbuhuo.day09;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Author 不温卜火
 * Create 2022-04-07 10:08
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:标量函数（Scalar Functions）：注册自定义函数
 */
public class Flink03_UDF_ScalarFun2 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.将数据转为JavaBean，并指定WaterWark
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        // 2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3.将流转为表
        Table table = tableEnv.fromDataStream(waterSensorDStream);

        // 4.注册自定义函数
        tableEnv.createTemporarySystemFunction("idLen", MyUDF.class);

        // 5.先注册再使用
        table.select($("id"),call("idLen", $("id"))).execute().print();

        // TODO 3.启动执行
        env.execute();

    }

    // TODO 自定义一个标量函数，一进一出，获取输入id的字符串长度
    public static class MyUDF extends ScalarFunction{
        public Integer eval(String value){
            return value.length();
        }
    }
}

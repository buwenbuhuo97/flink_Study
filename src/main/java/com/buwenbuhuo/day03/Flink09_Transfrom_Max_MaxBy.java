package com.buwenbuhuo.day03;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-28 10:02
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:简单滚动聚合算子介绍
 */
public class Flink09_Transfrom_Max_MaxBy {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.map的匿名内部类写法
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map((MapFunction<String, WaterSensor>) value -> {
            String[] split = value.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        // 2.先对相同的id进行聚合
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        // 3.使用聚合算子
        SingleOutputStreamOperator<WaterSensor> max = keyedStream.max("vc");
        SingleOutputStreamOperator<WaterSensor> maxBy1 = keyedStream.maxBy("vc",true);
        SingleOutputStreamOperator<WaterSensor> maxBy2 = keyedStream.maxBy("vc",false);

        // 4.打印
        max.print("max");

        maxBy1.print("maxby1");

        maxBy2.print("maxby2");

        // TODO 3.启动执行
        env.execute();
    }
}

package com.buwenbuhuo.day03;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-28 14:08
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:shuffle的lambda实现
 */
public class Flink06_Transfrom_Shuffle2 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(4);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.使用map将从端口读进来的字符串转为JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(input -> {
            String[] split = input.split(" ");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }).setParallelism(2);

        // 2.使用shuffle将id随机的分到不同分区中
        DataStream<WaterSensor> shuffle = map.shuffle();

        // 3.打印
        map.print("原始数据").setParallelism(2);
        shuffle.print("shuffle");


        // TODO 3.启动执行
        env.execute();
    }
}

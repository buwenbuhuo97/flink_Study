package com.buwenbuhuo.day03;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-28 21:15
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 对流重新分区的几个算子demo
 */
public class Flink12_Transfrom_Repartition {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.并行度设置
        env.setParallelism(4);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.堆数据做一个map操作
        SingleOutputStreamOperator<String> map = streamSource.map(r -> r);

        // 2.调用对流重分区的算子
        KeyedStream<String, String> keyedStream = map.keyBy(r -> r);

        DataStream<String> shuffle = map.shuffle();

        DataStream<String> rebalance = map.rebalance();

        DataStream<String> rescale = map.rescale();

        // 3.打印输出
        map.print("原始数据").setParallelism(2);
        keyedStream.print("keyBy");
        shuffle.print("shuffle");
        rebalance.print("rebalance");
        rescale.print("rescale");


        // TODO 3.启动执行
        env.execute();

    }
}

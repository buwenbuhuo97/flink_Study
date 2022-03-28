package com.buwenbuhuo.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-28 10:22
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:flatMap的lambda实现
 */
public class Flink03_Transfrom_flatMap3 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.通过flatMap将每一个单词取出来
        SingleOutputStreamOperator<String> flatMap = streamSource.flatMap((FlatMapFunction<String, String>) (input, out) -> {
            String[] words = input.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        })      // 注意类型擦除
                .returns(Types.STRING);
        // 2.打印输出
        flatMap.print();


        // TODO 3.启动执行
        env.execute();
    }
}

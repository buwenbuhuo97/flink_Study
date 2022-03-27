package com.buwenbuhuo.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Author 不温卜火
 * Create 2022-03-23 20:11
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:WordCount的流处理（无界流）精简版写法
 */
public class Flink07_Stream_Unbounded_WordCount {
    public static void main(String[] args) throws Exception {
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);

        // 3.读取文件流
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // 4.分词转换并分组统计
        SingleOutputStreamOperator<Tuple2<String, Long>> result = streamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)
                .sum(1);

        // 5.输出打印
        result.print();

        // 6.启动执行
        env.execute();
    }
}
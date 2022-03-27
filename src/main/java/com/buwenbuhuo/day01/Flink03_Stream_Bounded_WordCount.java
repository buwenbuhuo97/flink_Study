package com.buwenbuhuo.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Author 不温卜火
 * Create 2022-03-23 19:28
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: WordCount的流处理（有界流）匿名实现类写法
 */
public class Flink03_Stream_Bounded_WordCount {
    public static void main(String[] args) throws Exception {
        // 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);

        // 3.从文件中读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/words.txt");

        // 4.转换计算
        SingleOutputStreamOperator<String> wordDstream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                // 将数据按照空格切分
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        // 5.将打散出来的单词组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDstream = wordDstream.
                map(value -> Tuple2.of(value, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));;

        // 6.将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = wordToOneDstream.keyBy(0);

        // 7. 使用累加计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyBy.sum(1);

        // 8.输出打印
        result.print();

        // 9.启动执行
        env.execute();
    }
}

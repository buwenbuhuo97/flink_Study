package com.buwenbuhuo.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Author 不温卜火
 * Create 2022-03-23 20:11
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:共享组
 */
public class Flink01_Stream_Unbounded_WordCount {
    public static void main(String[] args) throws Exception {
        // 1.创建流式执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);

        // 3.读取无界流数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // 4.分词转换并分组统计
        SingleOutputStreamOperator<String> wordDstream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                // 将数据按照空格切分
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        })
                // 开辟一个新的共享组
                .slotSharingGroup("group1");

        // 5. 将每个单词组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDStream = wordDstream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        // 6.将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDStream.
                keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // 7. 做累加计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        // 8.打印输出
        result.print();

        // 9.启动执行
        env.execute();
    }
}

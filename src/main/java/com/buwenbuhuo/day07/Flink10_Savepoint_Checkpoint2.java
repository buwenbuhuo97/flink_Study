package com.buwenbuhuo.day07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Author 不温卜火
 * Create 2022-04-02 15:07
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:cancel的时候不会删除checkpoint信息
 */
public class Flink10_Savepoint_Checkpoint2 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置用户权限
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/flink0402/ck"));

        //从CK位置恢复数据，在代码中开启cancel的时候不会删除checkpoint信息这样就可以根据checkpoint来回复数据了
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup
                .RETAIN_ON_CANCELLATION);


        // TODO 开启CK
        // 每 5000ms 开始一次 checkpoint
        env.enableCheckpointing(5000);
        // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //2.读取端口数据并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = env.socketTextStream("hadoop01", 7777)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                });
        //3.按照单词分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordToOneDStream.keyBy(r -> r.f0);

        //4.累加计算
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.sum(1);

        //5.打印
        result.print();

        //6.开启任务
        env.execute();

    }

}

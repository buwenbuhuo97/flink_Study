package com.buwenbuhuo.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Arrays;
import java.util.List;

/**
 * Author 不温卜火
 * Create 2022-03-27 18:02
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:从集合中读取数据
 */
public class Flink02_Source_Collcetion {
    public static void main(String[] args) throws Exception {
        // TODO 1.创建执行环境
        // 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.全局并行度设置为1
        env.setParallelism(1);

        // TODO 2.从集合获取数据
        /**
         * 集合无法设置多并行度，
         * 设置会报错:The parallelism of non parallel operator must be 1.
         */
        /*
        // 1.创建集合
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        // 2.从集合获取数据
        DataStreamSource<Integer> streamSource = env.fromCollection(list);
        // DataStreamSource<Integer> streamSource = env.fromCollection(list).setParallelism(2);
        */

        /**
         * 多并行度设置方法：fromParallelCollection
         *  参数作用：
         *    @param iterator The iterator that produces the elements of the data stream
         *    @param typeInfo The TypeInformation for the produced data stream.
         *    @param <OUT> The type of the returned data stream
         */
        // env.fromParallelCollection();

        // 1.从元素中获取数据
        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5);

        // 此方法也不能设置多并行度
        // DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5).setParallelism(2);

        // 2.打印集合
        streamSource.print();

        // TODO 3.启动执行
        env.execute();
    }
}

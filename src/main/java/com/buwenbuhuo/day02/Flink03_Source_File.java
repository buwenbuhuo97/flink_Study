package com.buwenbuhuo.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-27 18:22
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:从文件读取数据
 */
public class Flink03_Source_File {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从文件中读取数据,从文件中读取是可以设置多并行度的
        DataStreamSource<String> streamSource = env.readTextFile("input/words.txt").setParallelism(2);
        // 4.打印输出
        streamSource.print();

        // TODO 2.启动执行
        env.execute();
    }
}

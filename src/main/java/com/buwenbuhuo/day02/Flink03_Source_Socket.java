package com.buwenbuhuo.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-27 18:33
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:从Socket读取数据
 */
public class Flink03_Source_Socket {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.读取无界流数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // 从Socket读取数据无法直接设置多并行度
        // DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777).setParallelism(2);

        // 4.打印输入数据
        streamSource.print();

        // TODO 2.启动执行
        env.execute();
    }
}

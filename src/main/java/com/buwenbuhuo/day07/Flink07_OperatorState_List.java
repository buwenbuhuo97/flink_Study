package com.buwenbuhuo.day07;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Author 不温卜火
 * Create 2022-04-02 11:06
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 算子状态:列表状态举例
 */
public class Flink07_OperatorState_List {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.获取两条流
        DataStreamSource<String> firstStreamSource = env.socketTextStream("hadoop01", 7777);
        DataStreamSource<String> secondStreamSource = env.socketTextStream("hadoop01", 8888);

        // TODO 2.核心代码


        // TODO 3.启动执行
        env.execute();
    }
}

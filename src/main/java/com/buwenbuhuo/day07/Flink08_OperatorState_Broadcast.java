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
 * Description: 算子状态:广播状态举例
 */
public class Flink08_OperatorState_Broadcast {
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
        // 1.定义一个状态并广播
        MapStateDescriptor<String, String> mapState = new MapStateDescriptor<>("map"
                , String.class, String.class);

        // 2.广播状态
        BroadcastStream<String> broadcast = firstStreamSource.broadcast(mapState);

        // 3.连接普通流和广播流
        BroadcastConnectedStream<String, String> connect = secondStreamSource.connect(broadcast);

        // 4.对两个流的数据进行处理
        SingleOutputStreamOperator<String> result = connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 提取广播状态
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapState);
                String swith = broadcastState.get("switch");
                if ("1".equals(swith)) {
                    out.collect("执行逻辑1......");
                } else if ("2".equals(swith)) {
                    out.collect("执行逻辑2......");
                } else {
                    out.collect("执行逻辑3......");
                }
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                // 提取状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapState);

                // 将数据放入广播状态
                broadcastState.put("switch", value);
            }
        });

        // 5. 输出并打印
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}
